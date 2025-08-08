from .lib.validation import MFTasksYamlImportConfig
from .lib.util import load_yaml_config

MANGO_FLOW_MODULES = []  # put it in the global scope


if mf_import_config := load_yaml_config(
    from_env_var="MANGO_FLOW_MODULES_CONFIG",
    from_path="config/mango_flow_modules.yaml",
    raise_error=False,
    pd_model=MFTasksYamlImportConfig,  # type: ignore
):
    MANGO_FLOW_MODULES = mf_import_config
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
        #     "module": ".tasks.exiftool_metadata_extraction",
        # },
        {
            "module": ".tasks.user_management",
        },
    ]
