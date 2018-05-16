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

func TestMetricIdCollision(t *testing.T) {
  a06, a10 := hash610("fm_rails_ruby_gc_freed_objects.hosts_controller.runtime")
  b06, b10 := hash610("fm_rails_ruby_gc_minor_count.config_groups_controller.index")

  if a06 == b06 {
    t.Errorf("cluster id collision: %v", a06)
  }
  if a10 != b10 {
    t.Errorf("expected metric ids to be same: %v vs %v", a10, b10)
  }
}
