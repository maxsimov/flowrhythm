from typing import Any, Dict, Hashable, List, Optional, Tuple, Protocol

from flowrhythm._types import Branch, Sink, Transformer


class Backend(Protocol):
    def set_producer(self, name: Hashable, handler: Transformer) -> None: ...
    def set_sink(self, name: Hashable, handler: Sink) -> None: ...
    def add_transform(
        self, name: Hashable, handler: Transformer, target: Hashable
    ) -> None: ...
    def add_branch(
        self, name: Hashable, handler: Branch, arms: List[Tuple[Hashable, Hashable]]
    ) -> None: ...


class BuilderError(Exception):
    pass


class Builder:
    def __init__(self):
        self._producer: Optional[Hashable] = None
        self._sink: Optional[Hashable] = None
        self._nodes: Dict[Hashable, Tuple[Any, str]] = {}
        self._arms: Dict[Hashable, Tuple[Hashable, Optional[Hashable]]] = {}
        self._branches: Dict[Hashable, Dict[str, Any]] = {}
        self._pending_arms: List[Tuple[Hashable, Hashable]] = []
        self._order: List[Tuple[str, Hashable]] = []
        self._context_stack: List[
            Tuple[Optional[Hashable], Optional[Hashable], Optional[Hashable]]
        ] = []

        self._branch_end: Dict[Hashable, int] = {}

        self._active_branch: Optional[Hashable] = None
        self._current_arm: Optional[Hashable] = None
        self._last_node: Optional[Hashable] = None

    def producer(self, name: Hashable, handler: Transformer):
        if self._producer is not None:
            raise BuilderError("Producer already defined")
        if name in self._nodes:
            raise BuilderError(f"Duplicate node name: {name}")
        self._producer = name
        self._nodes[name] = (handler, "producer")
        self._last_node = name
        self._order.append(("producer", name))
        return self

    def branch(self, name: Hashable, handler: Branch):
        if name in self._nodes or name in self._branches:
            raise BuilderError(f"Duplicate branch/node name: {name}")
        self._context_stack.append(
            (self._active_branch, self._current_arm, self._last_node)
        )
        self._branches[name] = {"handler": handler, "arms": {}}
        self._nodes[name] = (handler, "branch")
        self._active_branch = name
        self._current_arm = None
        self._last_node = None
        self._order.append(("branch", name))
        return self

    def arm(self, arm_name: Hashable):
        if arm_name in self._arms:
            raise BuilderError(f"Duplicate arm name: {arm_name}")
        if not self._active_branch:
            raise BuilderError("No active branch to add arms to")
        if arm_name in self._nodes or arm_name in self._branches:
            raise BuilderError(f"Arm name collides with node/branch name: {arm_name}")
        self._branches[self._active_branch]["arms"][arm_name] = None
        self._arms[arm_name] = (self._active_branch, None)
        self._current_arm = arm_name
        return self

    def then(self, name: Hashable, handler: Transformer):
        if name in self._nodes:
            raise BuilderError(f"Duplicate node name: {name}")
        self._nodes[name] = (handler, "transform")
        self._order.append(("transform", name))
        if self._active_branch and self._current_arm:
            self._branches[self._active_branch]["arms"][self._current_arm] = name
            self._arms[self._current_arm] = (self._active_branch, name)
        self._last_node = name
        self._current_arm = None
        return self

    def sink(self, name: Hashable, handler: Sink):
        if self._sink is not None:
            raise BuilderError("Sink already defined")
        if name in self._nodes:
            raise BuilderError(f"Duplicate node name: {name}")
        self._sink = name
        self._nodes[name] = (handler, "sink")
        self._order.append(("sink", name))
        if self._active_branch and self._current_arm:
            self._branches[self._active_branch]["arms"][self._current_arm] = name
            self._arms[self._current_arm] = (self._active_branch, name)
        self._last_node = name
        self._current_arm = None
        return self

    def jump(self, target_name: Hashable):
        if self._active_branch and self._current_arm:
            self._branches[self._active_branch]["arms"][self._current_arm] = target_name
            self._arms[self._current_arm] = (self._active_branch, target_name)
        self._current_arm = None
        return self

    def end(self):
        if not self._active_branch:
            raise BuilderError("No active branch to end")
        for arm, target in self._branches[self._active_branch]["arms"].items():
            if target is None:
                self._pending_arms.append((self._active_branch, arm))
        # Save end position for branch fallthrough
        self._branch_end[self._active_branch] = len(self._order)
        self._last_node = self._active_branch
        self._current_arm = None
        if self._context_stack:
            self._active_branch, self._current_arm, self._last_node = (
                self._context_stack.pop()
            )
        else:
            self._active_branch, self._current_arm, self._last_node = None, None, None
        return self

    def apply(self, backend: "Backend"):
        # Use branch end position to find correct fallthrough
        fallthrough_targets: Dict[Hashable, Hashable] = {}
        for branch_name, end_idx in self._branch_end.items():
            target = None
            for j in range(end_idx, len(self._order)):
                ntype, nname = self._order[j]
                if ntype in ("transform", "sink"):
                    target = nname
                    break
            if not target and self._sink:
                target = self._sink
            if not target:
                raise BuilderError(
                    f"No node or sink after branch '{branch_name}' for fallthrough arms"
                )
            fallthrough_targets[branch_name] = target

        for branch, arm in list(self._pending_arms):
            if (
                branch in fallthrough_targets
                and self._branches[branch]["arms"][arm] is None
            ):
                target = fallthrough_targets[branch]
                self._branches[branch]["arms"][arm] = target
                self._arms[arm] = (branch, target)
                self._pending_arms.remove((branch, arm))

        if self._pending_arms:
            raise BuilderError(f"Unresolved arms: {self._pending_arms}")

        # Check that all referenced targets exist
        referenced_targets = set()
        for type_, name in self._order:
            if type_ == "branch":
                for arm, target in self._branches[name]["arms"].items():
                    referenced_targets.add(target)
        if self._sink:
            referenced_targets.add(self._sink)
        for target in referenced_targets:
            if target is None:
                raise BuilderError("Null target for arm")
            if target not in self._nodes and target != self._sink:
                raise BuilderError(f"Target for jump/fallthrough not defined: {target}")

        backend.set_producer(self._producer, self._nodes[self._producer][0])  # type: ignore
        for type_, name in self._order:
            if type_ == "branch":
                arms = [
                    (arm, target)
                    for arm, target in self._branches[name]["arms"].items()
                ]
                backend.add_branch(name, self._branches[name]["handler"], arms)
            elif type_ == "transform":
                handler = self._nodes[name][0]
                backend.add_transform(name, handler, name)
            elif type_ == "sink":
                backend.set_sink(name, self._nodes[name][0])

        self.__init__()
