package security

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

func Sign(secret string, unixTime int64, body []byte) string {
	ts := strconv.FormatInt(unixTime, 10)
	mac := hmac.New(sha256.New , []byte(secret))
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(body)
	return "t=" + ts + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}
