package main

import (
  "testing"
)

func assertHash(t *testing.T, text string, xcluster uint32, xmetric uint32, xfull uint32, cluster uint32, metric uint32, full uint32) {
  if xcluster != cluster {
    t.Errorf("cluster hash of %v returned unexpected result %v (expected %v)", text, cluster, xcluster)
  }
  if xmetric != metric {
    t.Errorf("metric hash of %v returned unexpected result %v (expected %v)", text, metric, xmetric)
  }
  if xfull != full {
    t.Errorf("full hash of %v returned unexpected result %v (expected %v)", text, full, xfull)
  }
}

func TestHash610(t *testing.T) {
  text := "test"
  x06, x10, xfull := hash610(text)
  assertHash(t, text, 112, 485, 29157, x06, x10, xfull)
}

func TestMetricIdCollision(t *testing.T) {
  a06, a10, afull := hash610("fm_rails_ruby_gc_freed_objects.hosts_controller.runtime")
  b06, b10, bfull := hash610("fm_rails_ruby_gc_minor_count.config_groups_controller.index")
  assertHash(t, "fm_rails_ruby_gc_freed_objects.hosts_controller.runtime", 56, 764, 15100, a06, a10, afull)
  assertHash(t, "fm_rails_ruby_gc_minor_count.config_groups_controller.index", 80, 764, 21244, b06, b10, bfull)
}
