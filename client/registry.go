package client

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	RegistryPrefix = "/registry/"
	ConfigPrefix   = "/config/"
)

func checkName(kind, name string) error {
	if name == "" || strings.Contains(name, "/") {
		return fmt.Errorf("client: %s name %q must be non-empty and without a slash", kind, name)
	}
	return nil
}

func (c *Client) Mirror(ctx context.Context, prefix string) <-chan map[string]KeyValue {
	out := make(chan map[string]KeyValue, 1)
	go func() {
		defer close(out)
		backoff := c.opts.Backoff
		for ctx.Err() == nil {
			kvs, rev, err := c.RangeRev(ctx, prefix, 0)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
				}
				backoff = min(2*backoff, time.Second)
				continue
			}
			backoff = c.opts.Backoff
			view := make(map[string]KeyValue, len(kvs))
			for _, kv := range kvs {
				view[kv.Key] = kv
			}
			if !send(ctx, out, maps.Clone(view)) {
				return
			}
			for r := range c.Watch(ctx, prefix, true, rev+1) {
				if r.CompactRev != 0 {
					break
				}
				for _, ev := range r.Events {
					if ev.Type == EventDelete {
						delete(view, ev.KV.Key)
					} else {
						view[ev.KV.Key] = ev.KV
					}
				}
				if len(r.Events) > 0 && !send(ctx, out, maps.Clone(view)) {
					return
				}
			}
		}
	}()
	return out
}

func send[T any](ctx context.Context, out chan<- T, v T) bool {
	select {
	case out <- v:
		return true
	case <-ctx.Done():
		return false
	}
}

type Instance struct {
	Name string
	Addr string
}

func (c *Client) Discover(ctx context.Context, service string) (<-chan []Instance, error) {
	if err := checkName("service", service); err != nil {
		return nil, err
	}
	prefix := RegistryPrefix + service + "/"
	out := make(chan []Instance, 1)
	go func() {
		defer close(out)
		for view := range c.Mirror(ctx, prefix) {
			list := make([]Instance, 0, len(view))
			for k, kv := range view {
				list = append(list, Instance{Name: strings.TrimPrefix(k, prefix), Addr: string(kv.Value)})
			}
			slices.SortFunc(list, func(a, b Instance) int { return strings.Compare(a.Name, b.Name) })
			if !send(ctx, out, list) {
				return
			}
		}
	}()
	return out, nil
}

type Registration struct {
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	lease  uint64
	renews int
}

func (r *Registration) Lease() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lease
}

func (r *Registration) Renewals() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.renews
}

func (c *Client) register(ctx context.Context, key, addr string, ttl time.Duration) (Lease, error) {
	l, err := c.Grant(ctx, ttl)
	if err != nil {
		return Lease{}, err
	}
	if _, err := c.Put(ctx, key, []byte(addr), WithLease(l.ID)); err != nil {
		return Lease{}, err
	}
	return l, nil
}

func (c *Client) Register(ctx context.Context, service, instance, addr string, ttl time.Duration) (*Registration, error) {
	if err := errors.Join(checkName("service", service), checkName("instance", instance)); err != nil {
		return nil, err
	}
	key := RegistryPrefix + service + "/" + instance
	l, err := c.register(ctx, key, addr, ttl)
	if err != nil {
		return nil, err
	}
	rctx, cancel := context.WithCancel(context.Background())
	r := &Registration{cancel: cancel, done: make(chan struct{}), lease: l.ID}
	go func() {
		defer close(r.done)
		for {
			select {
			case <-rctx.Done():
				return
			case <-c.KeepAlive(rctx, l):
			}
			for rctx.Err() == nil {
				nl, err := c.register(rctx, key, addr, ttl)
				if err == nil {
					l = nl
					r.mu.Lock()
					r.lease, r.renews = l.ID, r.renews+1
					r.mu.Unlock()
					break
				}
				select {
				case <-rctx.Done():
				case <-time.After(ttl / 3):
				}
			}
		}
	}()
	return r, nil
}

func (c *Client) Deregister(ctx context.Context, r *Registration) error {
	r.cancel()
	<-r.done
	err := c.Revoke(ctx, r.Lease())
	if errors.Is(err, ErrSessionExpired) {
		err = c.Revoke(ctx, r.Lease())
	}
	if errors.Is(err, ErrLeaseNotFound) {
		return nil
	}
	return err
}

func (r *Registration) Abandon() {
	r.cancel()
	<-r.done
}

func configKey(service, key string) (string, error) {
	if err := errors.Join(checkName("service", service), checkName("config key", key)); err != nil {
		return "", err
	}
	return ConfigPrefix + service + "/" + key, nil
}

func (c *Client) SetConfig(ctx context.Context, service, key, value string) (uint64, error) {
	k, err := configKey(service, key)
	if err != nil {
		return 0, err
	}
	return c.Put(ctx, k, []byte(value))
}

func (c *Client) GetConfig(ctx context.Context, service, key string) (string, bool, error) {
	k, err := configKey(service, key)
	if err != nil {
		return "", false, err
	}
	kv, found, err := c.Get(ctx, k, ReadIndex)
	return string(kv.Value), found, err
}

func (c *Client) WatchConfig(ctx context.Context, service string) (<-chan map[string]string, error) {
	if err := checkName("service", service); err != nil {
		return nil, err
	}
	prefix := ConfigPrefix + service + "/"
	out := make(chan map[string]string, 1)
	go func() {
		defer close(out)
		for view := range c.Mirror(ctx, prefix) {
			cfg := make(map[string]string, len(view))
			for k, kv := range view {
				cfg[strings.TrimPrefix(k, prefix)] = string(kv.Value)
			}
			if !send(ctx, out, cfg) {
				return
			}
		}
	}()
	return out, nil
}
