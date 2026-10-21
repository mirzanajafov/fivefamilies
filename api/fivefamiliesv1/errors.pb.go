package fivefamiliesv1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
	unsafe "unsafe"
)

const (
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)

	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ErrorReason int32

const (
	ErrorReason_ERROR_REASON_UNSPECIFIED     ErrorReason = 0
	ErrorReason_ERROR_REASON_NOT_LEADER      ErrorReason = 1
	ErrorReason_ERROR_REASON_SESSION_EXPIRED ErrorReason = 2
	ErrorReason_ERROR_REASON_STALE_SEQUENCE  ErrorReason = 3
	ErrorReason_ERROR_REASON_TOO_MANY_LEASES ErrorReason = 4
	ErrorReason_ERROR_REASON_LEASE_NOT_FOUND ErrorReason = 5
)

var (
	ErrorReason_name = map[int32]string{
		0: "ERROR_REASON_UNSPECIFIED",
		1: "ERROR_REASON_NOT_LEADER",
		2: "ERROR_REASON_SESSION_EXPIRED",
		3: "ERROR_REASON_STALE_SEQUENCE",
		4: "ERROR_REASON_TOO_MANY_LEASES",
		5: "ERROR_REASON_LEASE_NOT_FOUND",
	}
	ErrorReason_value = map[string]int32{
		"ERROR_REASON_UNSPECIFIED":     0,
		"ERROR_REASON_NOT_LEADER":      1,
		"ERROR_REASON_SESSION_EXPIRED": 2,
		"ERROR_REASON_STALE_SEQUENCE":  3,
		"ERROR_REASON_TOO_MANY_LEASES": 4,
		"ERROR_REASON_LEASE_NOT_FOUND": 5,
	}
)

func (x ErrorReason) Enum() *ErrorReason {
	p := new(ErrorReason)
	*p = x
	return p
}

func (x ErrorReason) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ErrorReason) Descriptor() protoreflect.EnumDescriptor {
	return file_fivefamilies_v1_errors_proto_enumTypes[0].Descriptor()
}

func (ErrorReason) Type() protoreflect.EnumType {
	return &file_fivefamilies_v1_errors_proto_enumTypes[0]
}

func (x ErrorReason) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

func (ErrorReason) EnumDescriptor() ([]byte, []int) {
	return file_fivefamilies_v1_errors_proto_rawDescGZIP(), []int{0}
}

var File_fivefamilies_v1_errors_proto protoreflect.FileDescriptor

const file_fivefamilies_v1_errors_proto_rawDesc = "" +
	"\n" +
	"\x1cfivefamilies/v1/errors.proto\x12\x0ffivefamilies.v1*\xcf\x01\n" +
	"\vErrorReason\x12\x1c\n" +
	"\x18ERROR_REASON_UNSPECIFIED\x10\x00\x12\x1b\n" +
	"\x17ERROR_REASON_NOT_LEADER\x10\x01\x12 \n" +
	"\x1cERROR_REASON_SESSION_EXPIRED\x10\x02\x12\x1f\n" +
	"\x1bERROR_REASON_STALE_SEQUENCE\x10\x03\x12 \n" +
	"\x1cERROR_REASON_TOO_MANY_LEASES\x10\x04\x12 \n" +
	"\x1cERROR_REASON_LEASE_NOT_FOUND\x10\x05BHZFgithub.com/mirzanajafov/fivefamilies/api/fivefamiliesv1;fivefamiliesv1b\x06proto3"

var (
	file_fivefamilies_v1_errors_proto_rawDescOnce sync.Once
	file_fivefamilies_v1_errors_proto_rawDescData []byte
)

func file_fivefamilies_v1_errors_proto_rawDescGZIP() []byte {
	file_fivefamilies_v1_errors_proto_rawDescOnce.Do(func() {
		file_fivefamilies_v1_errors_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_fivefamilies_v1_errors_proto_rawDesc), len(file_fivefamilies_v1_errors_proto_rawDesc)))
	})
	return file_fivefamilies_v1_errors_proto_rawDescData
}

var file_fivefamilies_v1_errors_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_fivefamilies_v1_errors_proto_goTypes = []any{
	(ErrorReason)(0),
}
var file_fivefamilies_v1_errors_proto_depIdxs = []int32{
	0,
	0,
	0,
	0,
	0,
}

func init() { file_fivefamilies_v1_errors_proto_init() }
func file_fivefamilies_v1_errors_proto_init() {
	if File_fivefamilies_v1_errors_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_fivefamilies_v1_errors_proto_rawDesc), len(file_fivefamilies_v1_errors_proto_rawDesc)),
			NumEnums:      1,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_fivefamilies_v1_errors_proto_goTypes,
		DependencyIndexes: file_fivefamilies_v1_errors_proto_depIdxs,
		EnumInfos:         file_fivefamilies_v1_errors_proto_enumTypes,
	}.Build()
	File_fivefamilies_v1_errors_proto = out.File
	file_fivefamilies_v1_errors_proto_goTypes = nil
	file_fivefamilies_v1_errors_proto_depIdxs = nil
}
