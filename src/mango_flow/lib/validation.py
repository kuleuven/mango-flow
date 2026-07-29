# -*- coding: utf-8 -*-
"""This module contains validation functions and models for certain Mango Flow models and patterns.
mostly using Pydantic

@todo rename this module to validation_models instead
"""

from typing import Literal

from pydantic import BaseModel, Field, RootModel

########################################################################################
# Mango Flow beat Models
########################################################################################


class MFAction(BaseModel):
    name: str
    parameters: dict


class MFBeat(BaseModel):
    task: str
    schedule: int | str
    monitoring_offset: int | None = None
    client_user: str | None  = None
    path_input_mode: str = "single"


class MFMatch(BaseModel):
    subtree: str
    metadata: dict | None = None


class MFTrigger(BaseModel):
    beat: MFBeat


class MFSingleFlow(BaseModel):
    mode: Literal["single"]
    action: MFAction


class MFChainFlow(BaseModel):
    mode: Literal["chain"]
    actions: list[MFAction]


class MFChordFlow(BaseModel):
    mode: Literal["chord"]
    header_actions: list[MFAction]
    aggregate_actions: list[MFAction]


class MFBeatFlow(BaseModel):
    trigger: MFTrigger
    match: MFMatch
    flow: list[MFSingleFlow | MFChordFlow | MFChainFlow] = Field(discriminator="mode")


#########################################################################################
# ManGO Flow tasks import yaml/json configuration
#########################################################################################


class MFTaskImport(BaseModel):
    module: str
    package: str | None = None


class MFTasksYamlImportConfig(RootModel[list[MFTaskImport]]):
    """
    A model to validate the YAML configuration for Mango Flow tasks.
    The root is a list of individual imports as a flat dict.
    """
    pass
