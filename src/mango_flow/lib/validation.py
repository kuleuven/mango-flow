# -*- coding: utf-8 -*-
"""This module contains validation functions and models for certain Mango Flow models and patterns.
mostly using Pydantic
"""

from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, RootModel

########################################################################################
# Mango Flow beat Models
########################################################################################


class MFAction(BaseModel):
    name: str
    parameters: Dict


class MFBeat(BaseModel):
    task: str
    schedule: int | str
    monitoring_offset: Optional[int] = None
    client_user: Optional[str] = None
    path_input_mode: Optional[str] = "single"


class MFMatch(BaseModel):
    subtree: str
    metadata: Optional[dict] = None


class MFTrigger(BaseModel):
    beat: MFBeat


class MFSingleFlow(BaseModel):
    mode: Literal["single"]
    action: MFAction


class MFChainFlow(BaseModel):
    mode: Literal["chain"]
    actions: List[MFAction]


class MFChordFlow(BaseModel):
    mode: Literal["chord"]
    header_actions: List[MFAction]
    aggregate_actions: List[MFAction]


class MFBeatFlow(BaseModel):
    trigger: MFTrigger
    match: MFMatch
    flow: Union[MFSingleFlow, MFChordFlow, MFChainFlow] = Field(discriminator="mode")


#########################################################################################
# ManGO Flow tasks import yaml/json configuration
#########################################################################################


class MFTaskImport(BaseModel):
    module: str
    package: Optional[str] = None


class MFTasksYamlImportConfig(RootModel[List[MFTaskImport]]):
    """
    A model to validate the YAML configuration for Mango Flow tasks.
    The root is a list of individual imports as a flat dict.
    """
    pass
