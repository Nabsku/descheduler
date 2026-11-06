package descheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	fakeclientset "k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/descheduler/cmd/descheduler/app/options"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/utils"
	"sigs.k8s.io/descheduler/test"
)

func TestDryRunInitialPodsReadyBeforeFirstCycle(t *testing.T) {
	const podCount = 120
	objects := []runtime.Object{test.BuildTestNode("n1", 4000, 16<<30, 250, nil)}
	for i := 0; i < podCount; i++ {
		objects = append(objects, test.BuildTestPod(fmt.Sprintf("p-%03d", i), 10, 0, "n1", func(p *v1.Pod) {
			p.Status.Phase = v1.PodRunning
		}))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := fakeclientset.NewSimpleClientset(objects...)
	rs, err := options.NewDeschedulerServer()
	if err != nil {
		t.Fatal(err)
	}
	rs.Client = client
	rs.EventClient = client
	rs.DefaultFeatureGates = initFeatureGates()
	rs.DryRun = true
	realFactory := informers.NewSharedInformerFactoryWithOptions(client, 0, informers.WithTransform(trimManagedFields))
	broadcaster, recorder := utils.GetRecorderAndBroadcaster(ctx, client)
	defer broadcaster.Shutdown()
	d, _, err := bootstrapDescheduler(ctx, rs, &api.DeschedulerPolicy{}, "v1", realFactory, nil, recorder)
	if err != nil {
		t.Fatal(err)
	}

	immediate, err := d.getPodsAssignedToNode("n1", nil)
	if err != nil {
		t.Fatal(err)
	}
	var eventual int
	err = wait.PollUntilContextTimeout(ctx, 10*time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		pods, err := d.getPodsAssignedToNode("n1", nil)
		eventual = len(pods)
		return eventual == podCount, err
	})
	if err != nil {
		t.Fatalf("mirror did not converge: immediate=%d eventual=%d: %v", len(immediate), eventual, err)
	}
	if len(immediate) != podCount {
		t.Fatalf("first cycle sees %d/%d pods; mirror later converged to %d", len(immediate), podCount, eventual)
	}
}
