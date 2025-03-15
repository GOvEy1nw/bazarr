from __future__ import absolute_import
import logging
import os
import json
import time
import requests
from requests.exceptions import RequestException

from babelfish.language import Language as BabelfishLanguage
from subliminal_patch.subtitle import Subtitle
from subliminal_patch.providers import Provider
from subliminal.exceptions import ConfigurationError
from subliminal.video import Episode, Movie
from subzero.language import Language

logger = logging.getLogger(__name__)


def set_log_level(newLevel="INFO"):
    newLevel = newLevel.upper()
    logger.setLevel(getattr(logging, newLevel))


# initialize to default above
set_log_level()


class FileFlowsSubtitle(Subtitle):
    """FileFlows Subtitle."""

    provider_name = "fileflows"
    hash_verifiable = False

    def __init__(self, language, video):
        super(FileFlowsSubtitle, self).__init__(language)
        self.video = video
        self.workflow_id = None
        self.job_id = None

    @property
    def id(self):
        return f"{self.video.original_name}_{self.workflow_id}_{str(self.language)}"

    def get_matches(self, video):
        matches = set()
        if isinstance(video, Episode):
            matches.update(["series", "season", "episode"])
        elif isinstance(video, Movie):
            matches.update(["title"])
        return matches


class FileFlowsProvider(Provider):
    """FileFlows Provider."""

    # Support all languages since FileFlows processes files rather than providing subtitles
    languages = {Language.fromalpha2(l) for l in BabelfishLanguage.fromietf("en").codes}
    video_types = (Episode, Movie)

    def __init__(
        self, api_url=None, api_key=None, workflow_id=None, timeout=None, loglevel=None
    ):
        set_log_level(loglevel)

        if not api_url:
            raise ConfigurationError("FileFlows API URL must be provided")

        if not api_key:
            raise ConfigurationError("FileFlows API Key must be provided")

        if not workflow_id:
            raise ConfigurationError("FileFlows Workflow ID must be provided")

        if not timeout:
            raise ConfigurationError("FileFlows API timeout must be provided")

        self.api_url = api_url.rstrip("/")
        self.api_key = api_key
        self.workflow_id = workflow_id
        self.timeout = int(timeout)

        self.session = None

    def initialize(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            }
        )

    def terminate(self):
        if self.session:
            self.session.close()

    def ping(self):
        """Check if the FileFlows API is accessible."""
        try:
            response = self.session.get(f"{self.api_url}/api/status", timeout=10)
            return response.status_code == 200
        except RequestException:
            return False

    def submit_file_to_workflow(self, file_path):
        """Submit a file to a FileFlows workflow."""
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None

        try:
            payload = {"filePath": file_path, "workflowId": self.workflow_id}

            response = self.session.post(
                f"{self.api_url}/api/flow/process",
                data=json.dumps(payload),
                timeout=self.timeout,
            )

            if response.status_code == 200:
                job_data = response.json()
                logger.info(
                    f"File submitted to FileFlows: {file_path}, job ID: {job_data.get('uid')}"
                )
                return job_data.get("uid")
            else:
                logger.error(f"Failed to submit file to FileFlows: {response.text}")
                return None
        except RequestException as e:
            logger.error(f"Error submitting file to FileFlows: {str(e)}")
            return None

    def check_job_status(self, job_id):
        """Check the status of a FileFlows job."""
        try:
            response = self.session.get(
                f"{self.api_url}/api/flow/status/{job_id}", timeout=self.timeout
            )

            if response.status_code == 200:
                status_data = response.json()
                logger.debug(f"Job status: {status_data.get('status')}")
                return status_data
            else:
                logger.error(f"Failed to check job status: {response.text}")
                return None
        except RequestException as e:
            logger.error(f"Error checking job status: {str(e)}")
            return None

    def wait_for_completion(self, job_id, max_wait_time=3600):
        """Wait for a FileFlows job to complete with a timeout."""
        start_time = time.time()
        check_interval = 10  # seconds

        while time.time() - start_time < max_wait_time:
            status_data = self.check_job_status(job_id)
            if not status_data:
                return False

            status = status_data.get("status")

            if status == "Completed":
                logger.info(f"Job {job_id} completed successfully")
                return True
            elif status in ["Failed", "Cancelled"]:
                logger.error(f"Job {job_id} ended with status: {status}")
                return False

            # Still processing, wait and check again
            time.sleep(check_interval)
            # Gradually increase check interval to avoid excessive API calls
            check_interval = min(check_interval * 1.5, 60)

        logger.error(f"Job {job_id} timed out after {max_wait_time} seconds")
        return False

    def list_subtitles(self, video, languages):
        """List available subtitles for the given video and languages."""
        # FileFlows processes the file directly and doesn't list available subtitles
        # Instead, we create a placeholder subtitle object for each language
        subtitles = []
        for language in languages:
            subtitle = FileFlowsSubtitle(language, video)
            subtitle.workflow_id = self.workflow_id
            subtitles.append(subtitle)

        return subtitles

    def download_subtitle(self, subtitle):
        """Process the file through FileFlows workflow."""
        file_path = subtitle.video.original_path
        logger.info(f"Processing file with FileFlows: {file_path}")

        # Submit the file to the workflow
        job_id = self.submit_file_to_workflow(file_path)
        if not job_id:
            return

        subtitle.job_id = job_id

        # Wait for the job to complete
        success = self.wait_for_completion(job_id, self.timeout)

        if success:
            # The file was processed by FileFlows, but we don't return actual subtitle content
            # since FileFlows likely modified the original file or created a new file
            subtitle.content = (
                b""  # Empty content as we don't download an actual subtitle
            )
            logger.info(f"FileFlows successfully processed {file_path}")
        else:
            logger.error(f"FileFlows failed to process {file_path}")
            return
