"""
This module implements the dask class required to provide adaptive functionality
using Kubernetes.
"""

import logging, os

import distributed.deploy
from kubernetes import client, config


class KubeCluster(object):
    """
    Implementation of dask adaptive deployment where workers are spawned as
    Kubernetes pods.

    See http://distributed.readthedocs.io/en/latest/adaptive.html.
    """
    def __init__(self, **kwargs):
        self.log = logging.getLogger("distributed.deploy.adaptive")
        # Configure ourselves using environment variables
        if 'KUBECONFIG' in os.environ:
            kubernetes.config.load_kube_config()
        else:
            kubernetes.config.load_incluster_config()
        self.api = client.CoreV1Api()
        # Read the environment variables for configuration
        self.namespace = os.environ.get('NAMESPACE', 'dask')
        self.worker_labels = os.environ.get('WORKER_LABELS', 'app=dask,component=worker')
        dask_scheduler_service = os.environ.get('DASK_SCHEDULER_SERVICE', 'dask-scheduler')
        worker_name_prefix = os.environ.get('WORKER_NAME_PREFIX', 'dask-worker-')
        worker_image = os.environ.get('WORKER_IMAGE', 'daskdev/dask:latest')
        worker_image_pull_policy = os.environ.get('WORKER_IMAGE_PULL_POLICY', '')
        # Worker resources should be given as a string like "cpu=100m,memory=1Gi"
        # We need them as a dict
        worker_resources = dict(
            r.split('=') for r in os.environ.get('WORKER_RESOURCES', '').split(',')
        )
        # Build the pod template once for use later
        # Note that because we use generate_name rather than name, this is reusable
        self.pod_template = client.V1Pod(
            metadata = client.V1ObjectMeta(
                generate_name = worker_name_prefix,
                # Convert comma-separated 'k=v' pairs to a dict
                labels = dict(l.split('=') for l in self.worker_labels.split(','))
            ),
            spec = client.V1PodSpec(
                # We don't attempt to restart failed workers
                restart_policy = 'Never',
                containers = [
                    client.V1Container(
                        name = 'dask-worker',
                        image = worker_image,
                        image_pull_policy = worker_image_pull_policy,
                        env = [
                            client.V1EnvVar(
                                name = 'POD_IP',
                                value_from = client.V1EnvVarSource(
                                    field_ref = client.V1ObjectFieldSelector(
                                        field_path = 'status.podIP'
                                    )
                                )
                            ),
                            client.V1EnvVar(
                                name = 'POD_NAME',
                                value_from = client.V1EnvVarSource(
                                    field_ref = client.V1ObjectFieldSelector(
                                        field_path = 'metadata.name'
                                    )
                                )
                            ),
                        ],
                        args = [
                            'dask-worker'
                            dask_scheduler_service,
                            '--nprocs 1',
                            '--nthreads 1',
                            '--host $(POD_IP)',
                            '--name $(POD_NAME)',
                        ]
                    )
                ]
            )
        )
        # If resource requests were given, add them to the pod template
        if worker_resources:
            resources = client.V1ResourceRequirements(requests = worker_resources)
            self.pod_template.spec.containers[0].resources = resources

    def scale_up(self, n):
        """
        Creates worker pods so that the total number of worker pods is ``n``.
        """
        # Find the number of pods that match the specified labels
        try:
            pods = self.api.list_namespaced_pods(
                self.namespace,
                label_selector = self.worker_labels
            )
        except client.rest.ApiException:
            self.log.exception('Error fetching existing pods. No scaling attempted.')
            return
        self.log.info('Scaling worker pods from %s to %s', len(pods.items), n)
        for _ in xrange(n - len(pods.items)):
            # Create the pod
            try:
                created = self.api.create_namespaced_pod(self.namespace, self.pod_template)
            except client.rest.ApiException:
                self.log.exception('Error creating pod')
            else:
                self.log.info('Created pod: %s', created.metadata.name)

    def scale_down(self, workers):
        """
        This is a no-op. Kubernetes will automatically terminate the pods when
        the worker process exits.
        """


def dask_setup(scheduler):
    """
    Configures the dask scheduler to use the adaptive implementation.
    """
    cluster = KubeCluster()
    adapative_cluster = distributed.deploy.Adaptive(scheduler, cluster)
