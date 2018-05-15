package main

import (
  "testing"
)

func TestHash610(t *testing.T) {
  text := "test"
  x06, x10 := hash610(text)

  if x10 != 485 {
    t.Errorf("hash of %v returned unexpected result %v", text, x10)
  }
  if x06 != 28672 {
    t.Errorf("hash of %v returned unexpected result %v", text, x06)
  }
}
