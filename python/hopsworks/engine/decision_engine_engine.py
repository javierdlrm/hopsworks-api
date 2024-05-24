from abc import ABC, abstractmethod

import os
import logging

from hopsworks import client
from hopsworks.core import job_api, environment_api, dataset_api, kafka_api

from hsml.transformer import Transformer

from hsfs import connection as hsfs_conn
from hsml import connection as hsml_conn


class DecisionEngineEngine(ABC):

    def __init__(self):
        client.init("hopsworks")
        self._client = client.get_instance()

        self._dataset_api = dataset_api.DatasetApi(self._client._project_id)
        self._env_api = environment_api.EnvironmentApi(
            self._client._project_id, self._client._project_name
        )
        self._jobs_api = job_api.JobsApi(
            self._client._project_id, self._client._project_name
        )
        self._kafka_api = kafka_api.KafkaApi(
            self._client._project_id, self._client._project_name
        )
        self._fs = hsfs_conn().get_feature_store(
            self._client._project_name + "_featurestore"
        )
        self._mr = hsml_conn().get_model_registry()

    def setup_decision_engine(self, de):
        logging.info("[DecisionEngine] Setting up new decision engine")
        logging.info("[DecisionEngine] Installing requirements...")
        self.install_requirements(de)
        logging.info("[DecisionEngine] Creating jobs...")
        fp_job, sfp_job, tp_job = self.create_jobs(de)
        logging.info("[DecisionEngine] Starting jobs...")
        self.start_jobs(de, fp_job, sfp_job, tp_job)
        logging.info("[DecisionEngine] Creating deployments...")
        self.create_deployments(de)

    def get_common_resource_path(self, filename):
        return self.get_resource_path(filename=filename, de="common")

    def get_resource_path(self, filename, de=""):
        filename_segments = filename if isinstance(filename, list) else [filename]
        return os.path.join(
            "/Projects",
            self._client._project_name,
            "Resources",
            "decision-engine",
            de if isinstance(de, str) else de._name,
            *filename_segments,
        )

    def backup_common_resource_file(self, filename, de):
        original_filepath = self.get_common_resource_path(filename=filename)
        dest_filepath = self.get_resource_path(filename=filename, de=de)
        self._dataset_api.copy(original_filepath, dest_filepath, overwrite=True)
        return dest_filepath

    @abstractmethod
    def install_requirements(self, de):
        pass

    @abstractmethod
    def create_jobs(self, de):
        pass

    @abstractmethod
    def start_jobs(self, de):
        pass

    @abstractmethod
    def create_deployments(self, de):
        pass


class RecommendationDecisionEngineEngine(DecisionEngineEngine):

    def install_requirements(self, de):
        """Install dependencies required for the Recommendation Decision Engine."""
        dependencies_file_path = self.get_resource_path("requirements.txt")
        env = self._env_api.get_environment()
        env.install_requirements(dependencies_file_path, await_installation=True)

    def create_jobs(self, de):
        """Create the jobs that compose the Recommendation Decision Engine."""
        # create ingestion pipeline job (items)
        fp_filepath = self.backup_common_resource_file(
            filename="feature_pipeline.py", de=de
        )
        spark_config = self._jobs_api.get_configuration("PYSPARK")
        spark_config["appPath"] = fp_filepath
        spark_config["defaultArgs"] = f"-name {de._name}"
        fp_job = self._jobs_api.create_job(
            de._prefix + "feature_pipeline", spark_config
        )

        # create streaming feature pipeline job (events)
        sfp_filepath = self.backup_common_resource_file(
            filename="streaming_feature_pipeline.py", de=de
        )
        spark_config = self._jobs_api.get_configuration("PYSPARK")
        spark_config["appPath"] = sfp_filepath
        spark_config["defaultArgs"] = f"-name {de._name}"
        sfp_job = self._jobs_api.create_job(
            de._prefix + "streaming_feature_pipeline", spark_config
        )
        # - schedule streaming feature pipeline
        sfp_job.schedule(
            cron_expression="0 0 23 * * ?"
        )  # TODO: should run continuously instead

        # create training pipeline
        tp_filepath = self.backup_common_resource_file(
            filename="training_pipeline.py", de=de
        )
        py_config = self._jobs_api.get_configuration("PYTHON")
        py_config["appPath"] = tp_filepath
        py_config["defaultArgs"] = f"-name {de._name}"
        tp_job = self._jobs_api.create_job(de._prefix + "training_pipeline", py_config)
        retrain_config = de._configs_dict["model_configuration"]["retrain"]
        cron_schedule = (
            retrain_config["parameter"]
            if retrain_config["type"] == "time_based"
            else "0 0 0 * * ?"
        )
        # - schedule training pipeline job
        tp_job.schedule(cron_expression=cron_schedule)

        return fp_job, sfp_job, tp_job

    def start_jobs(self, de, fp_job, sfp_job, tp_job):
        """Orchestrate the jobs to ingest data and train a first version of the models."""
        # start streaming feature pipeline in the background
        sfp_job.run()
        # start and wait for data ingestion
        fp_job.run(await_termination=True)
        # trigger training pipeline once
        tp_job.run(await_termination=True)

    def create_deployments(self, de):
        """Create deployments for the query model and redirect of events to Kafka"""
        # create deployment for query model
        mr_query_model = self._mr.get_model(name=de._prefix + "query_model", version=1)
        transformer_script_path = self.backup_common_resource_file(
            filename=["inference_pipeline", "query_model_transformer.py"], de=de
        )
        query_transformer = Transformer(
            script_file=transformer_script_path, resources={"num_instances": 1}
        )
        _ = mr_query_model.deploy(
            name=(de._prefix + "query_deployment").replace("_", "").lower(),
            description="Deployment that computes query embedding from session activity and finds closest candidates",
            resources={"num_instances": 1},
            transformer=query_transformer,
        )

        # create deployment for redirect events endpoint
        redirect_model = self._mr.python.create_model(
            de._prefix + "events_redirect",
            description="Model for redirecting events into Kafka",
        )
        redirector_script_path = self.backup_common_resource_file(
            filename=["inference_pipeline", "events_redirect_predictor.py"], de=de
        )
        redirect_model.save(redirector_script_path, keep_original_files=True)
        redirector_script_path = os.path.join(
            redirect_model.version_path, "events_redirect_predictor.py"
        )
        _ = redirect_model.deploy(
            name=(de._prefix + "events_redirect_deployment").replace("_", "").lower(),
            script_file=redirector_script_path,
            resources={"num_instances": 1},
            description="Deployment that redirects session activity to Kafka",
        )

        try:
            self._kafka_api._delete_topic(de._kafka_topic_name)
        except Exception:
            pass
        _ = self._kafka_api.create_topic(
            de._kafka_topic_name, de._kafka_schema_name, 1, replicas=1, partitions=1
        )


class SearchDecisionEngineEngine(DecisionEngineEngine):

    def install_requirements(self, de):
        pass

    def create_jobs(self, de):
        pass

    def start_jobs(self, de):
        pass

    def create_deployments(self, de):
        # Implement logic to deploy search engine models
        pass
