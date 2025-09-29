package deployment

import (
	"context"
	"fmt"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/kmp"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kserve/kserve/pkg/apis/serving/v1beta1"
	"github.com/kserve/kserve/pkg/constants"
	"github.com/kserve/kserve/pkg/utils"
)

// RolloutReconciler manages Argo Rollout resources in lieu of Kubernetes Deployments.
type RolloutReconciler struct {
	client       kclient.Client
	scheme       *runtime.Scheme
	RolloutList  []*v1alpha1.Rollout
	componentExt *v1beta1.ComponentExtensionSpec
}

func NewRolloutReconciler(client kclient.Client,
	scheme *runtime.Scheme,
	componentMeta metav1.ObjectMeta,
	workerComponentMeta metav1.ObjectMeta,
	componentExt *v1beta1.ComponentExtensionSpec,
	podSpec *corev1.PodSpec, workerPodSpec *corev1.PodSpec,
	deployConfig *v1beta1.DeployConfig,
) (*RolloutReconciler, error) {
	deploymentList, err := createRawDeployment(componentMeta, workerComponentMeta, componentExt, podSpec, workerPodSpec, deployConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create base deployment for rollout: %w", err)
	}

	rollouts := make([]*v1alpha1.Rollout, 0, len(deploymentList))
	for _, dep := range deploymentList {
		rollouts = append(rollouts, convertDeploymentToRollout(dep, componentExt))
	}

	return &RolloutReconciler{
		client:       client,
		scheme:       scheme,
		RolloutList:  rollouts,
		componentExt: componentExt,
	}, nil
}

func convertDeploymentToRollout(dep *appsv1.Deployment, componentExt *v1beta1.ComponentExtensionSpec) *v1alpha1.Rollout {
	rollout := &v1alpha1.Rollout{
		ObjectMeta: dep.ObjectMeta,
		Spec: v1alpha1.RolloutSpec{
			Replicas: dep.Spec.Replicas,
			Selector: dep.Spec.Selector,
			Template: dep.Spec.Template,
			Strategy: v1alpha1.RolloutStrategy{
				Canary: &v1alpha1.CanaryStrategy{
					StableService: dep.Name,
					CanaryService: fmt.Sprintf("%s-canary", dep.Name),
				},
			},
		},
	}

	if dep.Spec.Template.Labels == nil {
		rollout.Spec.Template.Labels = map[string]string{}
	}

	if dep.Spec.Strategy.RollingUpdate != nil {
		ru := dep.Spec.Strategy.RollingUpdate
		rollout.Spec.Strategy.Canary.MaxSurge = ru.MaxSurge
		rollout.Spec.Strategy.Canary.MaxUnavailable = ru.MaxUnavailable
	}

	if componentExt != nil && componentExt.CanaryTrafficPercent != nil {
		percent := int32(ptr.Deref(componentExt.CanaryTrafficPercent, 0))
		rollout.Spec.Strategy.Canary.Steps = []v1alpha1.CanaryStep{{
			SetWeight: &percent,
		}, {
			Pause: &v1alpha1.RolloutPause{},
		}}
	}

	// Preserve labels/annotations from Deployment metadata
	return rollout
}

func (r *RolloutReconciler) ControllerObjects() []kclient.Object {
	objs := make([]kclient.Object, 0, len(r.RolloutList))
	for _, rollout := range r.RolloutList {
		objs = append(objs, rollout)
	}
	return objs
}

func (r *RolloutReconciler) Reconcile(ctx context.Context) ([]*appsv1.Deployment, error) {
	statusDeployments := make([]*appsv1.Deployment, 0, len(r.RolloutList))

	for _, desiredRollout := range r.RolloutList {
		checkResult, existingRollout, err := r.checkRolloutExist(ctx, r.client, desiredRollout)
		if err != nil {
			return nil, err
		}

		var opErr error
		switch checkResult {
		case constants.CheckResultCreate:
			opErr = r.client.Create(ctx, desiredRollout)
		case constants.CheckResultUpdate:
			cur := existingRollout.DeepCopy()
			mod := desiredRollout.DeepCopy()
			if mod.Annotations[constants.AutoscalerClass] != string(constants.AutoscalerClassNone) {
				mod.Spec.Replicas = nil
				cur.Spec.Replicas = nil
			}
			opErr = r.client.Patch(ctx, mod, kclient.MergeFrom(cur))
			if opErr == nil {
				existingRollout = mod
			}
		case constants.CheckResultDelete:
			if existingRollout.GetDeletionTimestamp() == nil {
				opErr = r.client.Delete(ctx, existingRollout)
			}
		}

		if opErr != nil {
			return nil, opErr
		}

		statusSource := desiredRollout
		if existingRollout != nil {
			statusSource = existingRollout
		}
		statusDeployments = append(statusDeployments, convertRolloutStatus(statusSource))
	}

	return statusDeployments, nil
}

func (r *RolloutReconciler) checkRolloutExist(ctx context.Context, client kclient.Client, rollout *v1alpha1.Rollout) (constants.CheckResultType, *v1alpha1.Rollout, error) {
	forceStopRuntime := utils.GetForceStopRuntime(rollout)
	existingRollout := &v1alpha1.Rollout{}

	err := client.Get(ctx, types.NamespacedName{Namespace: rollout.Namespace, Name: rollout.Name}, existingRollout)
	if err != nil {
		if apierr.IsNotFound(err) {
			if !forceStopRuntime {
				return constants.CheckResultCreate, nil, nil
			}
			return constants.CheckResultSkipped, nil, nil
		}
		return constants.CheckResultUnknown, nil, err
	}

	if forceStopRuntime {
		ctrl := metav1.GetControllerOf(rollout)
		existingCtrl := metav1.GetControllerOf(existingRollout)
		if ctrl != nil && existingCtrl != nil && ctrl.UID == existingCtrl.UID {
			return constants.CheckResultDelete, existingRollout, nil
		}
	}

	ignoreFields := cmpopts.IgnoreFields(v1alpha1.RolloutSpec{}, "Replicas")
	if rollout.Annotations[constants.AutoscalerClass] == string(constants.AutoscalerClassNone) {
		ignoreFields = nil
	}

	if err := client.Update(ctx, rollout, kclient.DryRunAll); err != nil {
		return constants.CheckResultUnknown, nil, err
	}

	if diff, err := kmp.SafeDiff(rollout.Spec, existingRollout.Spec, ignoreFields); err != nil {
		return constants.CheckResultUnknown, nil, err
	} else if diff != "" {
		return constants.CheckResultUpdate, existingRollout, nil
	}

	rolloutAnnotations := rollout.GetAnnotations()
	existingAnnotations := existingRollout.GetAnnotations()
	if diff := cmp.Diff(rolloutAnnotations, existingAnnotations); diff != "" {
		return constants.CheckResultUpdate, existingRollout, nil
	}

	return constants.CheckResultExisted, existingRollout, nil
}

func convertRolloutStatus(rollout *v1alpha1.Rollout) *appsv1.Deployment {
	deployment := &appsv1.Deployment{
		ObjectMeta: rollout.ObjectMeta,
		Status: appsv1.DeploymentStatus{
			Replicas:          rollout.Status.Replicas,
			ReadyReplicas:     rollout.Status.ReadyReplicas,
			UpdatedReplicas:   rollout.Status.UpdatedReplicas,
			AvailableReplicas: rollout.Status.AvailableReplicas,
		},
	}

	for _, cond := range rollout.Status.Conditions {
		deployment.Status.Conditions = append(deployment.Status.Conditions, convertRolloutCondition(cond))
	}

	return deployment
}

func convertRolloutCondition(cond v1alpha1.RolloutCondition) appsv1.DeploymentCondition {
	deploymentCond := appsv1.DeploymentCondition{
		Message:            cond.Message,
		Reason:             cond.Reason,
		LastTransitionTime: cond.LastTransitionTime,
	}

	switch cond.Type {
	case v1alpha1.RolloutAvailable:
		deploymentCond.Type = appsv1.DeploymentAvailable
	case v1alpha1.RolloutProgressing:
		deploymentCond.Type = appsv1.DeploymentProgressing
	case v1alpha1.RolloutDegraded:
		deploymentCond.Type = appsv1.DeploymentReplicaFailure
	default:
		deploymentCond.Type = appsv1.DeploymentConditionType(string(cond.Type))
	}

	switch cond.Status {
	case v1alpha1.RolloutConditionStatusTrue:
		deploymentCond.Status = corev1.ConditionTrue
	case v1alpha1.RolloutConditionStatusFalse:
		deploymentCond.Status = corev1.ConditionFalse
	default:
		deploymentCond.Status = corev1.ConditionUnknown
	}

	return deploymentCond
}
