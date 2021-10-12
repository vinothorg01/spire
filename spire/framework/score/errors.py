from abc import ABC


class ScoringError(ABC, Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class MLFlowDependenceError(ScoringError):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


# TODO(Max): Add tests in test_score and integrations/stages
class MissingFeaturesError(ScoringError):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class MissingHistoryError(ScoringError):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
