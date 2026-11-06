package descheduler

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	fakeclientset "k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/descheduler/cmd/descheduler/app/options"
	"sigs.k8s.io/descheduler/pkg/framework/plugins/defaultevictor"
	"sigs.k8s.io/descheduler/test"
)

func TestPodsWithPDBRemainEvictable(t *testing.T) {
	initPluginRegistry()
	n1 := test.BuildTestNode("n1", 2000, 3000, 10, nil)
	n2 := test.BuildTestNode("n2", 2000, 3000, 10, nil)
	makePod := func(name string) *v1.Pod {
		p := test.BuildTestPod(name, 100, 0, n1.Name, nil)
		p.ObjectMeta.OwnerReferences = test.GetReplicaSetOwnerRefList()
		p.Labels = map[string]string{"app": "foo"}
		return p
	}
	client := fakeclientset.NewSimpleClientset(n1, n2, makePod("p1"), makePod("p2"), makePod("p3"), test.BuildTestPDB("foo-pdb", "foo"))
	rs, err := options.NewDeschedulerServer()
	if err != nil {
		t.Fatal(err)
	}
	rs.Client = client
	rs.EventClient = client
	rs.DefaultFeatureGates = initFeatureGates()
	policy := removeDuplicatesPolicy()
	policy.Profiles[0].PluginConfigs[1].Args = &defaultevictor.DefaultEvictorArgs{
		PodProtections: defaultevictor.PodProtections{ExtraEnabled: []defaultevictor.PodProtection{defaultevictor.PodsWithoutPDB}},
	}
	var evicted []string
	client.PrependReactor("create", "pods", podEvictionReactionTestingFnc(&evicted, nil, nil))
	if err := RunDeschedulerStrategies(context.Background(), rs, policy, "v1"); err != nil {
		t.Fatal(err)
	}
	if len(evicted) == 0 {
		t.Fatal("PodsWithoutPDB blocked every pod despite a matching PDB")
	}
}
