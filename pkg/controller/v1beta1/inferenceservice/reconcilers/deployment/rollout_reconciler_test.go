package deployment

import (
	"context"
	"testing"

	rolloutsv1alpha1 "github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kserve/kserve/pkg/apis/serving/v1beta1"
	"github.com/kserve/kserve/pkg/constants"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rolloutsv1alpha1.AddToScheme(scheme))
	return scheme
}

func newComponentMeta() metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      "test-predictor",
		Namespace: "default",
		Labels: map[string]string{
			constants.DeploymentMode:  string(constants.Standard),
			constants.AutoscalerClass: string(constants.DefaultAutoscalerClass),
		},
		Annotations: map[string]string{},
	}
}

func newPodSpec() *corev1.PodSpec {
	return &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name:  constants.InferenceServiceContainerName,
			Image: "kserve/test:latest",
		}},
	}
}

func TestNewRolloutReconcilerBuildsCanaryStrategy(t *testing.T) {
	scheme := newTestScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).Build()

	componentExt := &v1beta1.ComponentExtensionSpec{
		CanaryTrafficPercent: ptr.To(int64(25)),
	}

	reconciler, err := NewRolloutReconciler(
		client,
		scheme,
		newComponentMeta(),
		metav1.ObjectMeta{},
		componentExt,
		newPodSpec(),
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, reconciler.RolloutList, 1)

	rollout := reconciler.RolloutList[0]
	assert.Equal(t, "test-predictor", rollout.Spec.Strategy.Canary.StableService)
	assert.Equal(t, "test-predictor-canary", rollout.Spec.Strategy.Canary.CanaryService)
	require.Len(t, rollout.Spec.Strategy.Canary.Steps, 2)
	assert.Equal(t, int32(25), ptr.Deref(rollout.Spec.Strategy.Canary.Steps[0].SetWeight, -1))
}

func TestRolloutReconcilerCreatesRollout(t *testing.T) {
	scheme := newTestScheme(t)
	client := fake.NewClientBuilder().WithScheme(scheme).Build()

	reconciler, err := NewRolloutReconciler(
		client,
		scheme,
		newComponentMeta(),
		metav1.ObjectMeta{},
		&v1beta1.ComponentExtensionSpec{},
		newPodSpec(),
		nil,
		nil,
	)
	require.NoError(t, err)

	deployments, err := reconciler.Reconcile(context.Background())
	require.NoError(t, err)
	require.Len(t, deployments, 1)

	created := &rolloutsv1alpha1.Rollout{}
	err = client.Get(context.Background(), types.NamespacedName{Name: "test-predictor", Namespace: "default"}, created)
	require.NoError(t, err)
	assert.Equal(t, reconciler.RolloutList[0].Spec.Template.Spec, created.Spec.Template.Spec)
}

func TestRolloutReconcilerConvertsStatus(t *testing.T) {
	scheme := newTestScheme(t)

	baseReconciler, err := NewRolloutReconciler(
		fake.NewClientBuilder().WithScheme(scheme).Build(),
		scheme,
		newComponentMeta(),
		metav1.ObjectMeta{},
		&v1beta1.ComponentExtensionSpec{},
		newPodSpec(),
		nil,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, baseReconciler.RolloutList, 1)

	existing := baseReconciler.RolloutList[0].DeepCopy()
	existing.Status = rolloutsv1alpha1.RolloutStatus{
		Replicas:          3,
		ReadyReplicas:     2,
		UpdatedReplicas:   3,
		AvailableReplicas: 2,
		Conditions: []rolloutsv1alpha1.RolloutCondition{{
			Type:    rolloutsv1alpha1.RolloutAvailable,
			Status:  corev1.ConditionTrue,
			Reason:  "AllGood",
			Message: "rollout is healthy",
		}},
	}

	baseReconciler.client = fake.NewClientBuilder().WithScheme(scheme).WithObjects(existing.DeepCopy()).Build()

	deployments, err := baseReconciler.Reconcile(context.Background())
	require.NoError(t, err)
	require.Len(t, deployments, 1)
	deploymentStatus := deployments[0].Status
	require.Equal(t, int32(3), deploymentStatus.Replicas)
	require.Equal(t, int32(2), deploymentStatus.ReadyReplicas)
	require.Len(t, deploymentStatus.Conditions, 1)
	assert.Equal(t, appsv1.DeploymentAvailable, deploymentStatus.Conditions[0].Type)
	assert.Equal(t, corev1.ConditionTrue, deploymentStatus.Conditions[0].Status)
}
