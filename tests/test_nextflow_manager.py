import tempfile
import os
from snowflakecli.nextflow.manager import NextflowManager
from snowflake.cli.api.exceptions import CliError
import pytest


def test_nextflow_manager_run_async(mock_db):
    # Create nextflow.config content with test profile
    config_content = """
plugins {
    id 'nf-snowflake@0.8.0'
}

profiles {
    test {
        snowflake {
            computePool = 'test'
            workDirStage = 'data_stage'
            stageMounts = 'input:/data/input,output:/data/output'
            enableStageMountV2 = true
        }
    }
}
"""

    # Create temporary directory with nextflow.config file
    with tempfile.TemporaryDirectory() as temp_dir:
        config_path = os.path.join(temp_dir, "nextflow.config")
        with open(config_path, "w") as f:
            f.write(config_content)

        manager = NextflowManager(
            project_dir=temp_dir,
            profile="test",
            id_generator=lambda: "abc1234",
            nf_snowflake_image="ghcr.io/snowflake-labs/nf-snowflake:0.8.0",
            temp_file_generator=lambda suffix: f"/tmp/tmp1234{suffix}",
        )
        manager.run_async()

        executed_queries = mock_db.get_executed_queries()
        # Check that we have the expected number of queries
        assert len(executed_queries) == 3

        # Check that the PUT command uses the deterministic file name
        put_query = executed_queries[0]
        assert put_query.startswith("PUT file:///tmp/tmp1234.tar.gz @data_stage/abc1234")

        # Check that the query tag is set correctly
        query_tag = executed_queries[1]
        assert "alter session set query_tag" in query_tag
        assert '"NEXTFLOW_JOB_TYPE": "main"' in query_tag
        assert '"NEXTFLOW_RUN_ID": "abc1234"' in query_tag

        assert (
            executed_queries[2]
            == """
EXECUTE JOB SERVICE
IN COMPUTE POOL test
NAME = NXF_MAIN_abc1234
FROM SPECIFICATION $$
spec:
  containers:
  - command:
    - /bin/bash
    - -c
    - "\\n        mkdir -p /mnt/project\\n        cd /mnt/project\\n        tar -zxf\\
      \\ /mnt/workdir/tmp1234.tar.gz\\n\\n        nextflow run . -name abc1234 -ansi-log\\
      \\ False -profile test -work-dir /mnt/workdir -with-report /tmp/report.html -with-trace\\
      \\ /tmp/trace.txt -with-timeline /tmp/timeline.html\\n        cp /tmp/report.html\\
      \\ /mnt/workdir/report.html\\n        cp /tmp/trace.txt /mnt/workdir/trace.txt\\n\\
      \\        cp /tmp/timeline.html /mnt/workdir/timeline.html\\n        "
    image: ghcr.io/snowflake-labs/nf-snowflake:0.8.0
    name: nf-main
    volumeMounts:
    - mountPath: /data/input
      name: vol-1
    - mountPath: /data/output
      name: vol-2
    - mountPath: /mnt/workdir
      name: workdir
  volumes:
  - name: vol-1
    source: stage
    stageConfig:
      enableSymlink: true
      name: '@input'
  - name: vol-2
    source: stage
    stageConfig:
      enableSymlink: true
      name: '@output'
  - name: workdir
    source: stage
    stageConfig:
      enableSymlink: true
      name: '@data_stage/abc1234/'

$$
"""
        )


def test_version_validation_matching_versions(mock_db):
    """Test version validation when plugin and image versions match."""
    config_content = """
plugins {
    id 'nf-snowflake@0.8.0'
}

profiles {
    test {
        snowflake {
            computePool = 'test_pool'
            workDirStage = 'test_stage'
        }
    }
}
"""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_path = os.path.join(temp_dir, "nextflow.config")
        with open(config_path, "w") as f:
            f.write(config_content)

        # This should not raise an exception
        manager = NextflowManager(
            project_dir=temp_dir,
            profile="test",
            nf_snowflake_image="ghcr.io/snowflake-labs/nf-snowflake:0.8.0",
            id_generator=lambda: "abc1234",
            temp_file_generator=lambda suffix: f"/tmp/tmp1234{suffix}",
        )
        manager.run_async()


def test_version_validation_mismatched_versions(mock_db):
    """Test version validation when plugin and image versions don't match."""
    config_content = """
plugins {
    id 'nf-snowflake@0.8.0'
}

profiles {
    test {
        snowflake {
            computePool = 'test_pool'
            workDirStage = 'test_stage'
        }
    }
}
"""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_path = os.path.join(temp_dir, "nextflow.config")
        with open(config_path, "w") as f:
            f.write(config_content)

        # This should raise a CliError due to version mismatch
        with pytest.raises(CliError, match="Version mismatch detected"):
            manager = NextflowManager(
                project_dir=temp_dir,
                profile="test",
                nf_snowflake_image="ghcr.io/snowflake-labs/nf-snowflake:0.7.1",
                id_generator=lambda: "abc1234",
                temp_file_generator=lambda suffix: f"/tmp/tmp1234{suffix}",
            )
            manager.run_async()


def test_version_validation_no_plugin_configured(mock_db):
    """Test version validation when no nf-snowflake plugin is configured."""
    config_content = """
profiles {
    test {
        snowflake {
            computePool = 'test_pool'
            workDirStage = 'test_stage'
        }
    }
}
"""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_path = os.path.join(temp_dir, "nextflow.config")
        with open(config_path, "w") as f:
            f.write(config_content)

        # This should not raise an exception (no plugin to validate)
        manager = NextflowManager(
            project_dir=temp_dir,
            profile="test",
            nf_snowflake_image="ghcr.io/snowflake-labs/nf-snowflake:0.8.0",
            id_generator=lambda: "abc1234",
            temp_file_generator=lambda suffix: f"/tmp/tmp1234{suffix}",
        )

        with pytest.raises(CliError, match="nf-snowflake plugin not found in nextflow.config"):
            manager.run_async()


def test_version_validation_plugin_without_version(mock_db):
    """Test version validation when plugin doesn't specify a version."""
    config_content = """
plugins {
    id 'nf-snowflake'
}

profiles {
    test {
        snowflake {
            computePool = 'test_pool'
            workDirStage = 'test_stage'
        }
    }
}
"""
    with tempfile.TemporaryDirectory() as temp_dir:
        config_path = os.path.join(temp_dir, "nextflow.config")
        with open(config_path, "w") as f:
            f.write(config_content)

        # This should not raise an exception (no version to validate)
        manager = NextflowManager(
            project_dir=temp_dir,
            profile="test",
            nf_snowflake_image="ghcr.io/snowflake-labs/nf-snowflake:0.8.0",
            id_generator=lambda: "abc1234",
            temp_file_generator=lambda suffix: f"/tmp/tmp1234{suffix}",
        )

        with pytest.raises(CliError, match="nf-snowflake plugin version not specified in nextflow.config"):
            manager.run_async()


def test_version_extraction_from_image():
    """Test version extraction from various image name formats."""
    # Create a temporary manager just to test the version extraction method
    with tempfile.TemporaryDirectory() as temp_dir:
        config_path = os.path.join(temp_dir, "nextflow.config")
        with open(config_path, "w") as f:
            f.write("""
profiles {
    test {
        snowflake {
            computePool = 'test'
        }
    }
}
""")

        manager = NextflowManager(project_dir=temp_dir, profile="test")

        # Test various image name patterns
        assert manager._extract_version_from_image("nf-snowflake:0.8.0") == "0.8.0"
        assert manager._extract_version_from_image("ghcr.io/snowflake-labs/nf-snowflake:0.7.1") == "0.7.1"
        assert manager._extract_version_from_image("repo/nf-snowflake:1.2.3") == "1.2.3"
        assert manager._extract_version_from_image("nf-snowflake:0.8.0-beta") == "0.8.0-beta"
        assert manager._extract_version_from_image("nf-snowflake:latest") == "latest"

        # Test edge cases
        assert manager._extract_version_from_image("nf-snowflake") is None
        assert manager._extract_version_from_image("") is None
        assert manager._extract_version_from_image(None) is None
