package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
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

const maxNameLen = 128

var ErrInvalid = errors.New("client: invalid argument")

func checkName(kind, name string) error {
	if name == "" || strings.Contains(name, "/") || len(name) > maxNameLen {
		return fmt.Errorf("%w: %s name %q must be non-empty, without a slash and at most %d bytes", ErrInvalid, kind, name, maxNameLen)
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
	Meta map[string]string
}

type record struct {
	Addr string            `json:"addr"`
	Meta map[string]string `json:"meta,omitempty"`
}

func servicePrefix(base, service string) (string, error) {
	if err := checkName("service", service); err != nil {
		return "", err
	}
	return base + service + "/", nil
}

func (i Instance) String() string {
	s := i.Name + "=" + i.Addr
	if len(i.Meta) == 0 {
		return s
	}
	meta := make([]string, 0, len(i.Meta))
	for _, k := range slices.Sorted(maps.Keys(i.Meta)) {
		meta = append(meta, k+"="+i.Meta[k])
	}
	return s + " (" + strings.Join(meta, ", ") + ")"
}

func instances(prefix string, kvs iter.Seq[KeyValue]) []Instance {
	list := []Instance{}
	for kv := range kvs {
		name := strings.TrimPrefix(kv.Key, prefix)
		var r record
		if checkName("instance", name) != nil || json.Unmarshal(kv.Value, &r) != nil || r.Addr == "" {
			continue
		}
		list = append(list, Instance{Name: name, Addr: r.Addr, Meta: r.Meta})
	}
	slices.SortFunc(list, func(a, b Instance) int { return strings.Compare(a.Name, b.Name) })
	return list
}

func (c *Client) Discover(ctx context.Context, service string) (<-chan []Instance, error) {
	prefix, err := servicePrefix(RegistryPrefix, service)
	if err != nil {
		return nil, err
	}
	out := make(chan []Instance, 1)
	go func() {
		defer close(out)
		for view := range c.Mirror(ctx, prefix) {
			if !send(ctx, out, instances(prefix, maps.Values(view))) {
				return
			}
		}
	}()
	return out, nil
}

func (c *Client) Lookup(ctx context.Context, service string, index uint64, wait time.Duration) ([]Instance, uint64, error) {
	prefix, err := servicePrefix(RegistryPrefix, service)
	if err != nil {
		return nil, 0, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		room := time.Until(deadline)
		wait = min(wait, room-min(c.opts.AttemptTimeout, room/2))
	}
	if index > 0 {
		if err := c.waitChange(ctx, prefix, index, wait); err != nil {
			return nil, 0, err
		}
	}
	kvs, rev, err := c.RangeRev(ctx, prefix, 0)
	if err != nil {
		return nil, 0, err
	}
	return instances(prefix, slices.Values(kvs)), rev, nil
}

func (c *Client) waitChange(ctx context.Context, prefix string, index uint64, wait time.Duration) error {
	wctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for r := range c.Watch(wctx, prefix, true, index+1) {
		if len(r.Events) > 0 {
			return nil
		}
	}
	return ctx.Err()
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

func (c *Client) RegisterOnce(ctx context.Context, service string, inst Instance, ttl time.Duration) (Lease, error) {
	prefix, err := servicePrefix(RegistryPrefix, service)
	if err != nil {
		return Lease{}, err
	}
	if err := checkName("instance", inst.Name); err != nil {
		return Lease{}, err
	}
	if inst.Addr == "" {
		return Lease{}, fmt.Errorf("%w: instance %q has no address", ErrInvalid, inst.Name)
	}
	value, err := json.Marshal(record{Addr: inst.Addr, Meta: inst.Meta})
	if err != nil {
		return Lease{}, err
	}
	l, err := c.Grant(ctx, ttl)
	if err != nil {
		return Lease{}, err
	}
	if _, err := c.Put(ctx, prefix+inst.Name, value, WithLease(l.ID)); err != nil {
		return Lease{}, leaseErr(err)
	}
	return l, nil
}

func (c *Client) Register(ctx context.Context, service string, inst Instance, ttl time.Duration) (*Registration, error) {
	inst.Meta = maps.Clone(inst.Meta)
	l, err := c.RegisterOnce(ctx, service, inst, ttl)
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
			case err := <-c.KeepAlive(rctx, l):
				if !errors.Is(err, ErrLeaseNotFound) {
					return
				}
			}
			for rctx.Err() == nil {
				nl, err := c.RegisterOnce(rctx, service, inst, ttl)
				if err == nil {
					l = nl
					r.mu.Lock()
					r.lease, r.renews = l.ID, r.renews+1
					r.mu.Unlock()
					break
				}
				select {
				case <-rctx.Done():
				case <-time.After(l.TTL / 3):
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
	prefix, err := servicePrefix(ConfigPrefix, service)
	if err != nil {
		return "", err
	}
	if err := checkName("config key", key); err != nil {
		return "", err
	}
	return prefix + key, nil
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
	prefix, err := servicePrefix(ConfigPrefix, service)
	if err != nil {
		return nil, err
	}
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
