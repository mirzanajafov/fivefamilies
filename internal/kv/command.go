package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type Op uint8

const (
	OpRegister Op = iota + 1
	OpGet
	OpRange
	OpPut
	OpDelete
	OpCAS
	OpCASRev
	OpLeaseGrant
	OpLeaseRevoke
	OpLeaseExpire
)

var opNames = [...]string{
	OpRegister:    "register",
	OpGet:         "get",
	OpRange:       "range",
	OpPut:         "put",
	OpDelete:      "delete",
	OpCAS:         "cas",
	OpCASRev:      "casrev",
	OpLeaseGrant:  "lease-grant",
	OpLeaseRevoke: "lease-revoke",
	OpLeaseExpire: "lease-expire",
}

func (o Op) String() string {
	if int(o) < len(opNames) && opNames[o] != "" {
		return opNames[o]
	}
	return fmt.Sprintf("Op(%d)", uint8(o))
}

type Command struct {
	Op            Op
	Client        uint64
	Seq           uint64
	Key           string
	Value         []byte
	Expect        []byte
	ExpectMissing bool
	ExpectRev     uint64
	Limit         uint64
	Lease         uint64
	TTL           uint64
}

var ErrMalformed = errors.New("kv: malformed command")

func Encode(c Command) []byte {
	b := make([]byte, 0, 16+len(c.Key)+len(c.Value)+len(c.Expect))
	b = append(b, byte(c.Op))
	b = binary.AppendUvarint(b, c.Client)
	b = binary.AppendUvarint(b, c.Seq)
	b = appendBytes(b, []byte(c.Key))
	switch c.Op {
	case OpPut:
		b = appendBytes(b, c.Value)
	case OpCAS:
		b = appendBytes(b, c.Value)
		if c.ExpectMissing {
			b = append(b, 1)
		} else {
			b = append(b, 0)
			b = appendBytes(b, c.Expect)
		}
	case OpCASRev:
		b = appendBytes(b, c.Value)
		b = binary.AppendUvarint(b, c.ExpectRev)
	case OpRange:
		b = binary.AppendUvarint(b, c.Limit)
	case OpLeaseGrant:
		b = binary.AppendUvarint(b, c.TTL)
	case OpLeaseRevoke, OpLeaseExpire:
		b = binary.AppendUvarint(b, c.Lease)
	}
	if c.Lease != 0 && c.writes() {
		b = binary.AppendUvarint(b, c.Lease)
	}
	return b
}

func (c Command) writes() bool {
	return c.Op == OpPut || c.Op == OpCAS || c.Op == OpCASRev
}

func appendBytes(b, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

type decoder struct {
	b   []byte
	err bool
}

func (d *decoder) uvarint() uint64 {
	if d.err {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = true
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) bytes() []byte {
	n := d.uvarint()
	if d.err || n > uint64(len(d.b)) {
		d.err = true
		return nil
	}
	v := d.b[:n:n]
	d.b = d.b[n:]
	return v
}

func (d *decoder) byte() byte {
	if d.err || len(d.b) == 0 {
		d.err = true
		return 0
	}
	v := d.b[0]
	d.b = d.b[1:]
	return v
}

func Decode(data []byte) (Command, error) {
	d := decoder{b: data}
	var c Command
	c.Op = Op(d.byte())
	c.Client = d.uvarint()
	c.Seq = d.uvarint()
	c.Key = string(d.bytes())
	switch c.Op {
	case OpRegister, OpGet, OpDelete:
	case OpPut:
		c.Value = d.bytes()
	case OpCAS:
		c.Value = d.bytes()
		switch d.byte() {
		case 0:
			c.Expect = d.bytes()
		case 1:
			c.ExpectMissing = true
		default:
			d.err = true
		}
	case OpCASRev:
		c.Value = d.bytes()
		c.ExpectRev = d.uvarint()
	case OpRange:
		c.Limit = d.uvarint()
	case OpLeaseGrant:
		c.TTL = d.uvarint()
	case OpLeaseRevoke, OpLeaseExpire:
		c.Lease = d.uvarint()
	default:
		d.err = true
	}
	if c.writes() && len(d.b) > 0 {
		if c.Lease = d.uvarint(); c.Lease == 0 {
			d.err = true
		}
	}
	if d.err || len(d.b) != 0 {
		return Command{}, ErrMalformed
	}
	return c, nil
}
