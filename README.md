# pp_upstream

Post-processing project for upstream-viz.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

###  Prerequisites
1. [Miniconda](https://docs.conda.io/en/latest/miniconda.html)
2. [Conda-Pack](https://conda.github.io/conda-pack)
3. Python 3.9.7

### Installation
1. Create python 3.9.7  virtual environment,
   ```bash 
   conda create -p ./venv
   conda install -p ./venv python==3.9.7
   conda activate ./venv
   pip install --no-input -r ./requirements.txt  --extra-index-url http://s.dev.isgneuro.com/repository/ot.platform/simple --trusted-host s.dev.isgneuro.com
   ```      
2. Install [postprocessin sdk](https://github.com/ISGNeuroTeam/postprocessing_sdk)

3. Make symlinks to commands you want to run in `./venv/lib/python3.9/site-packages/postprocessing_sdk/pp_cmd`. Example:
   ```bash
   cd ./venv/lib/python3.9/site-packages/postprocessing_sdk/pp_cmd
   ln -s /pp_upstream/pp_upstream/commmands/analisys_criteria_filter analisys_criteria_filter  
   ```
4. Create `config.yaml` file and configure path to `data` directory. Example:   
   ```ini
   data:
     path: ./venv/lib/python3.9/site-packages/upstream_viz_lib/data/
   ```
5. Before run `pp` command set environment variables with path to `get_data.yaml` and `config.yaml`: 
   ```bash
   export UPSTREAM_VIZ_DATA_CONFIG=/work/pp_upstream/pp_upstream/get_data.yaml 
   export UPSTREAM_VIZ_CONFIG=/work/pp_upstream/pp_upstream/config.yaml 
   ```


## Deploy
1. Unpack archive into `commands` directory
2. Configure `config.yaml` in `commands/upstream_venv/` directory
    ```bash
      cd commands
      cp ./upstream_venv/config.example.yaml ./upstream_venv/config.yaml
    ```
3. Restart python_computing_node
