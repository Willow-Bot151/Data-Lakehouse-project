import copy
from typing import Final

from moto.stepfunctions.parser.asl.component.eval_component import EvalComponent
from moto.stepfunctions.parser.asl.eval.environment import Environment
from moto.stepfunctions.parser.asl.utils.json_path import JSONPathUtils


class ItemsPath(EvalComponent):
    DEFAULT_PATH: Final[str] = "$"

    def __init__(self, items_path_src: str = DEFAULT_PATH):
        self.items_path_src: Final[str] = items_path_src

    def _eval_body(self, env: Environment) -> None:
        if self.items_path_src != ItemsPath.DEFAULT_PATH:
            value = copy.deepcopy(env.stack[-1])
            value = JSONPathUtils.extract_json(self.items_path_src, value)
            env.stack.append(value)
