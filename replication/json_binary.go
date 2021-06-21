package replication

import (
	"bytes"
	"fmt"
	"math"

	. "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/segmentio/objconv"
	ojson "github.com/segmentio/objconv/json"
	"github.com/siddontang/go/hack"
)

const (
	JSONB_SMALL_OBJECT byte = iota // small JSON object
	JSONB_LARGE_OBJECT             // large JSON object
	JSONB_SMALL_ARRAY              // small JSON array
	JSONB_LARGE_ARRAY              // large JSON array
	JSONB_LITERAL                  // literal (true/false/null)
	JSONB_INT16                    // int16
	JSONB_UINT16                   // uint16
	JSONB_INT32                    // int32
	JSONB_UINT32                   // uint32
	JSONB_INT64                    // int64
	JSONB_UINT64                   // uint64
	JSONB_DOUBLE                   // double
	JSONB_STRING                   // string
	JSONB_OPAQUE       byte = 0x0f // custom data (any MySQL data type)
)

const (
	JSONB_NULL_LITERAL  byte = 0x00
	JSONB_TRUE_LITERAL  byte = 0x01
	JSONB_FALSE_LITERAL byte = 0x02
)

const (
	jsonbSmallOffsetSize = 2
	jsonbLargeOffsetSize = 4

	jsonbKeyEntrySizeSmall = 2 + jsonbSmallOffsetSize
	jsonbKeyEntrySizeLarge = 2 + jsonbLargeOffsetSize

	jsonbValueEntrySizeSmall = 1 + jsonbSmallOffsetSize
	jsonbValueEntrySizeLarge = 1 + jsonbLargeOffsetSize
)

func jsonbGetOffsetSize(isSmall bool) int {
	if isSmall {
		return jsonbSmallOffsetSize
	}

	return jsonbLargeOffsetSize
}

func jsonbGetKeyEntrySize(isSmall bool) int {
	if isSmall {
		return jsonbKeyEntrySizeSmall
	}

	return jsonbKeyEntrySizeLarge
}

func jsonbGetValueEntrySize(isSmall bool) int {
	if isSmall {
		return jsonbValueEntrySizeSmall
	}

	return jsonbValueEntrySizeLarge
}

// decodeJsonBinary decodes the JSON binary encoding data and returns
// the common JSON encoding data.
func (e *RowsEvent) decodeJsonBinary(data []byte) ([]byte, error) {
	// Sometimes, we can insert a NULL JSON even we set the JSON field as NOT NULL.
	// If we meet this case, we can return an empty slice.
	if len(data) == 0 {
		return []byte{}, nil
	}
	buffer := bytes.NewBuffer(nil)
	emitter := ojson.NewEmitter(buffer)
	d := jsonBinaryDecoder{
		useDecimal:      e.useDecimal,
		ignoreDecodeErr: e.ignoreJSONDecodeErr,

		emitter: emitter,
	}

	if d.isDataShort(data, 1) {
		return nil, d.err
	}

	d.emitValue(data[0], data[1:])
	if d.err != nil {
		return nil, d.err
	}

	return buffer.Bytes(), nil
}

type jsonBinaryDecoder struct {
	useDecimal      bool
	ignoreDecodeErr bool
	err             error

	emitter objconv.Emitter
}

func (d *jsonBinaryDecoder) emitValue(tp byte, data []byte) {
	if d.err != nil {
		return
	}

	switch tp {
	case JSONB_SMALL_OBJECT:
		d.emitObjectOrArray(data, true, true)
		return
	case JSONB_LARGE_OBJECT:
		d.emitObjectOrArray(data, false, true)
		return
	case JSONB_SMALL_ARRAY:
		d.emitObjectOrArray(data, true, false)
		return
	case JSONB_LARGE_ARRAY:
		d.emitObjectOrArray(data, false, false)
		return
	case JSONB_LITERAL:
		d.emitLiteral(data)
		return
	case JSONB_INT16:
		d.emitInt16(data)
		return
	case JSONB_UINT16:
		d.emitUint16(data)
		return
	case JSONB_INT32:
		d.emitInt32(data)
		return
	case JSONB_UINT32:
		d.emitUint32(data)
		return
	case JSONB_INT64:
		d.emitInt64(data)
		return
	case JSONB_UINT64:
		d.emitUint64(data)
		return
	case JSONB_DOUBLE:
		d.emitDouble(data)
		return
	case JSONB_STRING:
		d.emitString(data)
		return
	case JSONB_OPAQUE:
		// d.emitOpaque(data)
		return
	default:
		d.err = errors.Errorf("invalid json type %d", tp)
	}

	return
}

func (d *jsonBinaryDecoder) emitObjectOrArray(data []byte, isSmall bool, isObject bool) {
	offsetSize := jsonbGetOffsetSize(isSmall)
	if d.isDataShort(data, 2*offsetSize) {
		d.emitter.EmitNil()
		return
	}

	count := d.decodeCount(data, isSmall)
	size := d.decodeCount(data[offsetSize:], isSmall)

	if d.isDataShort(data, size) {
		// Before MySQL 5.7.22, json type generated column may have invalid value,
		// bug ref: https://bugs.mysql.com/bug.php?id=88791
		// As generated column value is not used in replication, we can just ignore
		// this error and return a dummy value for this column.
		if d.ignoreDecodeErr {
			d.err = nil
		}
		d.emitter.EmitNil()
		return
	}

	keyEntrySize := jsonbGetKeyEntrySize(isSmall)
	valueEntrySize := jsonbGetValueEntrySize(isSmall)

	headerSize := 2*offsetSize + count*valueEntrySize

	if isObject {
		headerSize += count * keyEntrySize
	}

	if headerSize > size {
		d.err = errors.Errorf("header size %d > size %d", headerSize, size)
		return
	}

	var keys []string
	if isObject {
		keys = make([]string, count)
		for i := 0; i < count; i++ {
			// decode key
			entryOffset := 2*offsetSize + keyEntrySize*i
			keyOffset := d.decodeCount(data[entryOffset:], isSmall)
			keyLength := int(d.decodeUint16(data[entryOffset+offsetSize:]))

			// Key must start after value entry
			if keyOffset < headerSize {
				d.err = errors.Errorf("invalid key offset %d, must > %d", keyOffset, headerSize)
				return
			}

			if d.isDataShort(data, keyOffset+keyLength) {
				d.emitter.EmitNil()
				return
			}

			keys[i] = hack.String(data[keyOffset : keyOffset+keyLength])
		}
	}

	if d.err != nil {
		return
	}

	if isObject {
		d.emitter.EmitMapBegin(count)
	} else {
		d.emitter.EmitArrayBegin(count)
	}

	for i := 0; i < count; i++ {
		if i != 0 {
			if isObject {
				d.emitter.EmitMapNext()
			} else {
				d.emitter.EmitArrayNext()
			}
		}

		if isObject {
			d.emitter.EmitString(keys[i])
			d.emitter.EmitMapValue()
		}

		// decode value
		entryOffset := 2*offsetSize + valueEntrySize*i
		if isObject {
			entryOffset += keyEntrySize * count
		}

		tp := data[entryOffset]

		if isInlineValue(tp, isSmall) {
			d.emitValue(tp, data[entryOffset+1:entryOffset+valueEntrySize])
			continue
		}

		valueOffset := d.decodeCount(data[entryOffset+1:], isSmall)

		if d.isDataShort(data, valueOffset) {
			d.err = errors.Errorf("short array value")
			return
		}

		d.emitValue(tp, data[valueOffset:])
	}

	if d.err != nil {
		return
	}

	if isObject {
		d.emitter.EmitMapEnd()
	} else {
		d.emitter.EmitArrayEnd()
	}

	return
}

func isInlineValue(tp byte, isSmall bool) bool {
	switch tp {
	case JSONB_INT16, JSONB_UINT16, JSONB_LITERAL:
		return true
	case JSONB_INT32, JSONB_UINT32:
		return !isSmall
	}

	return false
}

func (d *jsonBinaryDecoder) emitLiteral(data []byte) {
	if d.isDataShort(data, 1) {
		d.emitter.EmitNil()
		return
	}

	tp := data[0]

	switch tp {
	case JSONB_NULL_LITERAL:
		d.emitter.EmitNil()
		return
	case JSONB_TRUE_LITERAL:
		d.emitter.EmitBool(true)
		return
	case JSONB_FALSE_LITERAL:
		d.emitter.EmitBool(false)
		return
	}

	d.err = errors.Errorf("invalid literal %c", tp)

	return
}

func (d *jsonBinaryDecoder) isDataShort(data []byte, expected int) bool {
	if d.err != nil {
		return true
	}

	if len(data) < expected {
		d.err = errors.Errorf("data len %d < expected %d", len(data), expected)
	}

	return d.err != nil
}

func (d *jsonBinaryDecoder) decodeInt16(data []byte) int16 {
	if d.isDataShort(data, 2) {
		return 0
	}

	v := ParseBinaryInt16(data[0:2])
	return v
}

func (d *jsonBinaryDecoder) emitInt16(data []byte) {
	v := d.decodeInt16(data)
	d.emitter.EmitInt(int64(v), 16)
}

func (d *jsonBinaryDecoder) decodeUint16(data []byte) uint16 {
	if d.isDataShort(data, 2) {
		return 0
	}

	v := ParseBinaryUint16(data[0:2])
	return v
}

func (d *jsonBinaryDecoder) emitUint16(data []byte) {
	v := d.decodeUint16(data)
	d.emitter.EmitUint(uint64(v), 16)
}

func (d *jsonBinaryDecoder) decodeInt32(data []byte) int32 {
	if d.isDataShort(data, 4) {
		return 0
	}

	v := ParseBinaryInt32(data[0:4])
	return v
}

func (d *jsonBinaryDecoder) emitInt32(data []byte) {
	v := d.decodeInt32(data)
	d.emitter.EmitInt(int64(v), 32)
}

func (d *jsonBinaryDecoder) decodeUint32(data []byte) uint32 {
	if d.isDataShort(data, 4) {
		return 0
	}

	v := ParseBinaryUint32(data[0:4])
	return v
}

func (d *jsonBinaryDecoder) emitUint32(data []byte) {
	v := d.decodeUint32(data)
	d.emitter.EmitUint(uint64(v), 32)
}

func (d *jsonBinaryDecoder) decodeInt64(data []byte) int64 {
	if d.isDataShort(data, 8) {
		return 0
	}

	v := ParseBinaryInt64(data[0:8])
	return v
}

func (d *jsonBinaryDecoder) emitInt64(data []byte) {
	v := d.decodeInt64(data)
	d.emitter.EmitInt(v, 64)
}

func (d *jsonBinaryDecoder) decodeUint64(data []byte) uint64 {
	if d.isDataShort(data, 8) {
		return 0
	}

	v := ParseBinaryUint64(data[0:8])
	return v
}

func (d *jsonBinaryDecoder) emitUint64(data []byte) {
	v := d.decodeUint64(data)
	d.emitter.EmitUint(v, 64)
}

func (d *jsonBinaryDecoder) decodeDouble(data []byte) float64 {
	if d.isDataShort(data, 8) {
		return 0
	}

	v := ParseBinaryFloat64(data[0:8])
	return v
}

func (d *jsonBinaryDecoder) emitDouble(data []byte) {
	v := d.decodeDouble(data)
	d.emitter.EmitFloat(v, 64)
}

func (d *jsonBinaryDecoder) decodeString(data []byte) string {
	if d.err != nil {
		return ""
	}

	l, n := d.decodeVariableLength(data)

	if d.isDataShort(data, l+n) {
		return ""
	}

	data = data[n:]

	v := hack.String(data[0:l])
	return v
}

func (d *jsonBinaryDecoder) emitString(data []byte) {
	v := d.decodeString(data)
	d.emitter.EmitString(v)
}

func (d *jsonBinaryDecoder) emitOpaque(data []byte) {
	if d.isDataShort(data, 1) {
		d.emitter.EmitNil()
		return
	}

	tp := data[0]
	data = data[1:]

	l, n := d.decodeVariableLength(data)

	if d.isDataShort(data, l+n) {
		d.emitter.EmitNil()
		return
	}

	data = data[n : l+n]

	switch tp {
	// case MYSQL_TYPE_NEWDECIMAL:
	// 	d.emitDecimal(data)
	// case MYSQL_TYPE_TIME:
	// 	d.emitTime(data)
	// case MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP:
	// 	d.emitDateTime(data)
	default:
		d.emitter.EmitString(hack.String(data))
	}
}

func (d *jsonBinaryDecoder) emitDecimal(data []byte) interface{} {
	precision := int(data[0])
	scale := int(data[1])

	v, _, err := decodeDecimal(data[2:], precision, scale, d.useDecimal)
	d.err = err

	return v
}

func (d *jsonBinaryDecoder) emitTime(data []byte) interface{} {
	v := d.decodeInt64(data)

	if v == 0 {
		return "00:00:00"
	}

	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}

	intPart := v >> 24
	hour := (intPart >> 12) % (1 << 10)
	min := (intPart >> 6) % (1 << 6)
	sec := intPart % (1 << 6)
	frac := v % (1 << 24)

	return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, min, sec, frac)
}

func (d *jsonBinaryDecoder) emitDateTime(data []byte) interface{} {
	v := d.decodeInt64(data)
	if v == 0 {
		return "0000-00-00 00:00:00"
	}

	// handle negative?
	if v < 0 {
		v = -v
	}

	intPart := v >> 24
	ymd := intPart >> 17
	ym := ymd >> 5
	hms := intPart % (1 << 17)

	year := ym / 13
	month := ym % 13
	day := ymd % (1 << 5)
	hour := (hms >> 12)
	minute := (hms >> 6) % (1 << 6)
	second := hms % (1 << 6)
	frac := v % (1 << 24)

	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, frac)
}

func (d *jsonBinaryDecoder) decodeCount(data []byte, isSmall bool) int {
	if isSmall {
		v := d.decodeUint16(data)
		return int(v)
	}

	return int(d.decodeUint32(data))
}

func (d *jsonBinaryDecoder) decodeVariableLength(data []byte) (int, int) {
	// The max size for variable length is math.MaxUint32, so
	// here we can use 5 bytes to save it.
	maxCount := 5
	if len(data) < maxCount {
		maxCount = len(data)
	}

	pos := 0
	length := uint64(0)
	for ; pos < maxCount; pos++ {
		v := data[pos]
		length |= uint64(v&0x7F) << uint(7*pos)

		if v&0x80 == 0 {
			if length > math.MaxUint32 {
				d.err = errors.Errorf("variable length %d must <= %d", length, int64(math.MaxUint32))
				return 0, 0
			}

			pos += 1
			// TODO: should consider length overflow int here.
			return int(length), pos
		}
	}

	d.err = errors.New("decode variable length failed")

	return 0, 0
}
