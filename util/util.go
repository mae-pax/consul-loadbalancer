package util

import (
	crand "crypto/rand"
	"math/big"
	"math/rand"
	"os/exec"
	"strings"
	"time"
)

// Logger for log
type Logger interface {
	Debugf(format string, v ...interface{})
	Debug(args string)
	Infof(format string, v ...interface{})
	Info(args string)
	Warnf(format string, v ...interface{})
	Warn(args string)
	Errorf(format string, v ...interface{})
	Error(args string)
}

func Zone() string {
	out, err := exec.Command("/bin/bash", "-c", "/opt/aws/bin/ec2-metadata -z").Output()
	if err != nil {
		return "unknown"
	}

	kv := strings.Split(string(out[:len(out)-1]), " ")
	if len(kv) != 2 {
		return "unknown"
	}

	return kv[1]
}

func IntPseudoRandom(min, max int) int {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	return r.Intn(max-min+1) + min
}

func IntGenuineRandom(min, max int64) int64 {
	res, _ := crand.Int(crand.Reader, big.NewInt(max-min+1))
	return res.Int64() + min
}