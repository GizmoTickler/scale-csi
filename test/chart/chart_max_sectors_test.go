package chart

import (
	"strings"
	"testing"

	"github.com/GizmoTickler/scale-csi/pkg/util"
)

// nvmeof.maxSectorsKB renders into the driver ConfigMap only when it is not
// the drivers' default (auto), so a default install's ConfigMap is unchanged
// and a rolled-back binary (strict YAML) still parses it.
func TestNVMeoFMaxSectorsKBRendering(t *testing.T) {
	auto := util.NVMeMaxSectorsPolicy{Mode: util.NVMeMaxSectorsAuto}
	off := util.NVMeMaxSectorsPolicy{Mode: util.NVMeMaxSectorsOff}
	cases := []struct {
		name, values, want string
		policy             util.NVMeMaxSectorsPolicy
	}{
		{name: "default", values: "", want: "", policy: auto},
		{name: "explicit auto", values: "  maxSectorsKB: auto\n", want: "", policy: auto},
		{name: "integer", values: "  maxSectorsKB: 4096\n", want: `maxSectorsKB: "4096"`, policy: util.NVMeMaxSectorsPolicy{Mode: util.NVMeMaxSectorsFixed, LimitKB: 4096}},
		{name: "quoted integer", values: "  maxSectorsKB: \"2048\"\n", want: `maxSectorsKB: "2048"`, policy: util.NVMeMaxSectorsPolicy{Mode: util.NVMeMaxSectorsFixed, LimitKB: 2048}},
		{name: "off", values: "  maxSectorsKB: \"off\"\n", want: `maxSectorsKB: "off"`, policy: off},
		{name: "bare off (YAML 1.1 boolean)", values: "  maxSectorsKB: off\n", want: `maxSectorsKB: "false"`, policy: off},
		{name: "zero", values: "  maxSectorsKB: 0\n", want: `maxSectorsKB: "0"`, policy: off},
		{name: "deleted", values: "  maxSectorsKB: null\n", want: "", policy: auto},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			valuesPath := writeValues(t, "max-sectors.yaml", "nvmeof:\n  enabled: true\n  subsystemAllowAnyHost: true\n"+tc.values)
			out := helmTemplate(t, append([]string{"-f", valuesPath, "--show-only", "templates/configmap.yaml"}, requiredConfigArgs...)...)
			// What the driver makes of it (strict parse, as at startup).
			cfg := loadRenderedConfig(t, renderedConfigYAML(t, out))
			policy, err := util.ParseNVMeMaxSectorsPolicy(cfg.NVMeoF.MaxSectorsKB)
			if err != nil || policy != tc.policy {
				t.Errorf("driver reads %+v (%v), want %+v", policy, err, tc.policy)
			}
			if tc.want == "" {
				if strings.Contains(out, "maxSectorsKB") {
					t.Errorf("the default must render no maxSectorsKB key:\n%s", out)
				}
				return
			}
			if !strings.Contains(out, "      "+tc.want) {
				t.Errorf("want %q under nvmeof:\n%s", tc.want, out)
			}
		})
	}
}

func TestNVMeoFMaxSectorsKBSchemaRejectsNonsense(t *testing.T) {
	for _, bad := range []string{"3", "-1", "4k", "on", "\"4096KiB\"", "true"} {
		t.Run(bad, func(t *testing.T) {
			valuesPath := writeValues(t, "max-sectors-bad.yaml", "nvmeof:\n  enabled: true\n  maxSectorsKB: "+bad+"\n")
			out := helmTemplateExpectError(t, "-f", valuesPath)
			if !strings.Contains(out, "maxSectorsKB") {
				t.Errorf("rejection does not name the value:\n%s", out)
			}
		})
	}
}
