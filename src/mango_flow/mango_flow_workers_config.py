import os
import pathlib

import yaml

from .lib.validation import MFTasksYamlImportConfig

MANGO_FLOW_MODULES = []  # put it in the global scope

if os.environ.get("MANGO_FLOW_MODULES_CONFIG", None):
    try:
        mf_import_config = yaml.safe_load(
            pathlib.Path(os.environ["MANGO_FLOW_MODULES_CONFIG"]).read_text()
        )
        if MFTasksYamlImportConfig.model_validate(mf_import_config):
            MANGO_FLOW_MODULES = mf_import_config
    except Exception as e:
        raise RuntimeError(
            f"Failed to load Mango Flow workers configuration from YAML: {e}"
        )
else:
    MANGO_FLOW_MODULES = [
        {
            "module": ".tasks.basic_operations",
        },
        {
            "module": ".tasks.basic_metadata_extraction",
        },
        {
            "module": ".monitors",
        },
        {
            "module": ".tasks.reporting",
        },
        # {
        #     "module": ".tasks.fiber",
        # },
        # {
        #     "module": ".tasks.exiftool_metadata_extraction",
        # },
        {
            "module": ".tasks.user_management",
        },
        # {"module": ".tasks.mango_user_management"},
    ]
