# Snowflake CLI Nextflow Plugin

A Snowflake CLI plugin that enables running [Nextflow](https://www.nextflow.io/) workflows directly in [Snowpark Container Services](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview).

## Overview

This plugin extends the Snowflake CLI with Nextflow workflow capabilities, allowing you to:

- **Run Nextflow workflows at scale** using Snowflake's compute resources
- **Stream live execution logs** through secure WebSocket connections
- **Manage workflow execution** directly from your command line
- **Leverage Snowflake's security and governance** for bioinformatics and data pipelines

### Key Features

- 🚀 **Seamless Integration**: Run Nextflow workflows directly in Snowpark Container Services
- 📊 **Real-time Monitoring**: Stream live logs and execution status via WebSocket
- 🔒 **Enterprise Security**: Built-in Snowflake authentication and authorization
- 🎯 **Simple CLI Interface**: Familiar command-line experience for workflow management
- 📦 **Flexible Configuration**: Support for Nextflow profiles and custom configurations

## Getting Started

### Prerequisites

- All prerequisites of running snowflake-cli mentioned [here](https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation/installation#requirements)
- Access to a Snowflake account with Snowpark Container Services enabled
- A Snowflake compute pool configured for container workloads
- Nextflow binary correctly installed in your PATH

### Installation

1. **Install Snowflake CLI**
   ```bash
   pip install snowflake-cli
   ```

2. **Install the Nextflow plugin**
   ```bash
   pip install snowflake-cli-nextflow
   ```

3. **Configure Snowflake connection**
   ```bash
   snow connection add
   ```
   Follow the prompts to configure your Snowflake connection with appropriate credentials.

4. **Enable the Nextflow plugin**
   ```bash
   snow plugin enable nextflow
   ```
   The `nextflow` command should now be available in the CLI.

### Quick Start

1. **Prepare your Nextflow project**
   
   Create a `nextflow.config` file in your project directory:
   ```groovy
   // nextflow.config
   snowflake {
       computePool = 'YOUR_COMPUTE_POOL'
       workDirStage = 'WORKDIR_STAGE'
       stageMounts = 'INPUT:/mnt/input,OUTPUT:/mnt/output'
   }
   ```

2. **Upload nf-snowflake image into snowflake image repository**
  
   ```bash

   # Pull image from github container registry
   docker pull ghcr.io/snowflake-labs/nf-snowflake:latest
   
   # retag the image to snowflake registry
   docker tag ghcr.io/snowflake-labs/nf-snowflake:latest <snowflake_registry>

   # config snow nextflow to use the corresponding image
   snow nextflow config set -key nf_snowflake_image -value <snowflake_registry_image>
   ```

3. **Run your workflow**
   ```bash
   snow nextflow run /path/to/your/nextflow-project -profile snowflake
   ```

4. **Monitor execution**
   
   The plugin will automatically:
   - Upload your project to Snowflake
   - Submit the workflow to Snowpark Container Services
   - Stream live logs to your terminal

## Configuration

### Snowflake-specific Configuration

Your `nextflow.config` must include the following Snowflake-specific settings:

```groovy
plugins { id 'nf-snowflake@0.6.3' }

snowflake {
    computePool = 'YOUR_COMPUTE_POOL'
    workDirStage = 'WORKDIR_STAGE'
    stageMounts = 'INPUT:/mnt/input,OUTPUT:/mnt/output'
}
```

### Configuration Parameters

- **`computePool`** (Required): The name of your Snowflake compute pool where the Nextflow containers will run. This compute pool must be configured with appropriate resources and permissions for container workloads.

- **`workDirStage`** (Required): The Snowflake stage name where the plugin will upload your workflow files and store execution artifacts. This stage serves as the working directory for your Nextflow execution.

- **`stageMounts`** (Optional): A comma-separated list of stage mounts in the format `STAGE_NAME:/mount/path`. Each mount makes a Snowflake stage available inside the container at the specified path. Use this to provide input data and collect output results.

### Example with Multiple Stage Mounts

```groovy
snowflake {
    computePool = 'NEXTFLOW_POOL'
    workDirStage = 'NF_WORKSPACE'
    stageMounts = 'RAW_DATA:/mnt/input,RESULTS:/mnt/output,REFERENCES:/mnt/ref'
}
```

## Usage Examples

### Basic Workflow Execution

```bash
# Run with default profile
snow nextflow run ./my-workflow

# Run with specific profile
snow nextflow run ./my-workflow -profile snowflake

# Check available commands
snow nextflow --help
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Nextflow](https://www.nextflow.io/) - The workflow management system
- [Snowflake](https://www.snowflake.com/) - The cloud data platform
- [Snowpark Container Services](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview) - Container orchestration platform
