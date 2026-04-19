from unittest.mock import patch

from sqlalchemy.orm import Session

from ee.onyx.external_permissions.google_drive.group_sync import gdrive_group_sync
from onyx.configs.constants import DocumentSource
from onyx.connectors.google_utils.shared_constants import (
    DB_CREDENTIALS_AUTHENTICATION_METHOD,
)
from onyx.connectors.google_utils.shared_constants import (
    DB_CREDENTIALS_DICT_TOKEN_KEY,
)
from onyx.connectors.google_utils.shared_constants import (
    GoogleOAuthAuthenticationMethod,
)
from onyx.connectors.models import InputType
from onyx.db.enums import AccessType
from onyx.db.enums import ConnectorCredentialPairStatus
from onyx.db.models import Connector
from onyx.db.models import ConnectorCredentialPair
from onyx.db.models import Credential
from tests.external_dependency_unit.constants import TEST_TENANT_ID


def _make_user_oauth_cc_pair(db_session: Session) -> ConnectorCredentialPair:
    connector = Connector(
        name="Test GDrive User OAuth",
        source=DocumentSource.GOOGLE_DRIVE,
        input_type=InputType.POLL,
        connector_specific_config={"include_shared_drives": True},
        refresh_freq=None,
        prune_freq=None,
        indexing_start=None,
    )
    db_session.add(connector)
    db_session.flush()

    credential = Credential(
        source=DocumentSource.GOOGLE_DRIVE,
        credential_json={
            DB_CREDENTIALS_DICT_TOKEN_KEY: "{}",
            DB_CREDENTIALS_AUTHENTICATION_METHOD: (
                GoogleOAuthAuthenticationMethod.OAUTH_USER_INTERACTIVE.value
            ),
        },
        user_id=None,
    )
    db_session.add(credential)
    db_session.flush()
    db_session.expire(credential)

    cc_pair = ConnectorCredentialPair(
        connector_id=connector.id,
        credential_id=credential.id,
        name="Test User OAuth CC Pair",
        status=ConnectorCredentialPairStatus.ACTIVE,
        access_type=AccessType.SYNC,
        auto_sync_options=None,
    )
    db_session.add(cc_pair)
    db_session.commit()
    db_session.refresh(cc_pair)
    return cc_pair


def test_gdrive_group_sync_skips_when_user_oauth(db_session: Session) -> None:
    """Single-user OAuth credentials lack admin.directory.* scopes. Group
    sync must short-circuit before calling get_admin_service so the sync
    does not 403 mid-run."""
    cc_pair = _make_user_oauth_cc_pair(db_session)

    with patch(
        "ee.onyx.external_permissions.google_drive.group_sync.get_admin_service"
    ) as mock_admin_service, patch(
        "ee.onyx.external_permissions.google_drive.group_sync.GoogleDriveConnector"
    ) as mock_connector_cls:
        groups = list(gdrive_group_sync(TEST_TENANT_ID, cc_pair))

        assert groups == []
        mock_admin_service.assert_not_called()
        # Should skip before even loading connector creds
        mock_connector_cls.return_value.load_credentials.assert_not_called()
