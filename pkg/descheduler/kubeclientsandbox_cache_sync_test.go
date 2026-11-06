package descheduler

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/descheduler/test"
)

func TestDryRunStartupDoesNotRetainDeletedPod(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pod := test.BuildTestPod("deleted-during-startup", 100, 0, "n1", nil)
	realClient := fakeclientset.NewSimpleClientset(pod)
	realFactory := informers.NewSharedInformerFactory(realClient, 0)
	sandbox, err := newKubeClientSandbox(realClient, realFactory, v1.SchemeGroupVersion.WithResource("pods"))
	if err != nil {
		t.Fatal(err)
	}
	// Subscribe before mirroring real objects; a real-first fake List/Watch
	// gap can miss deletions.
	sandbox.fakeSharedInformerFactory().Start(ctx.Done())
	sandbox.fakeSharedInformerFactory().WaitForCacheSync(ctx.Done())
	realFactory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), sandbox.mirrorHasSynced...) {
		t.Fatal("mirror did not sync")
	}
	gvr := v1.SchemeGroupVersion.WithResource("pods")
	if err := wait.PollUntilContextTimeout(ctx, time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		return sandbox.hasObjectInIndexer(gvr, pod.Namespace, pod.Name, string(pod.UID))
	}); err != nil {
		t.Fatalf("initial pod was not mirrored: %v", err)
	}
	if err := realClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	key := pod.Namespace + "/" + pod.Name
	if err := wait.PollUntilContextTimeout(ctx, time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		_, exists, err := sandbox.resourceToInformer[gvr].Informer().GetIndexer().GetByKey(key)
		return !exists, err
	}); err != nil {
		t.Fatalf("real informer did not observe deletion: %v", err)
	}
	if err := wait.PollUntilContextTimeout(ctx, time.Millisecond, 5*time.Second, true, func(context.Context) (bool, error) {
		return sandbox.mirrorsInSync()
	}); err != nil {
		t.Fatalf("fake cache did not catch up after deletion: %v", err)
	}
	if exists, err := sandbox.hasObjectInIndexer(gvr, pod.Namespace, pod.Name, string(pod.UID)); err != nil || exists {
		t.Fatalf("deleted pod still cached: exists=%t err=%v", exists, err)
	}
}
