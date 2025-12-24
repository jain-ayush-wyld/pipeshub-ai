from app.config.constants.arangodb import AppGroups, Connectors
from app.connectors.core.interfaces.connector.apps import App


class GithubApp(App):
    def __init__(self) -> None:
        super().__init__(Connectors.GITHUB.value, AppGroups.GITHUB.value)
