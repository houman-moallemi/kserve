package deployment

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WorkloadReconciler abstracts raw workload reconciliation so that Deployments and other controllers
// such as Argo Rollouts can share the same high-level lifecycle management.
type WorkloadReconciler interface {
	ControllerObjects() []client.Object
	Reconcile(ctx context.Context) ([]*appsv1.Deployment, error)
}
