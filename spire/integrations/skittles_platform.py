import datetime
import getpass
import requests

from requests.exceptions import RequestException

STAGING = "https://segmentation-api-stag.conde.io/graphql"
PRODUCTION = "https://segmentation-api.conde.io/graphql"


class SkittlesMixin:

    MUTATION = "mutation {{ {mutation} }}"
    QUERY = "query {{ {query} }}"
    CREATE = "createWorkflow(request: {request} ) {schema}"
    UPDATE = "updateWorkflow(request: {request} ) {schema}"
    GET_WORKFLOW = "workflow(workflowId: {workflow_id} ) {schema}"

    SCHEMA = """
            {
                workflowId
                isWorkflowEnabled
                isWorkflowTrained
                traits {
                    traitId
                    name
                    vendor
                    type
                    attribs
                    configuration
                    createdBy
                    dateCreated
                    dateUpdated
                    signals
                    segments {
                        segment {
                            segmentId
                            systemCode
                            technicalStatus
                            isActive
                            segmentSize
                        }
                    }
                }
            }
                """

    def __post_to_graphql(self, request_data):
        api = self._assign_api_endpoint()
        response = requests.post(url=api, json={"query": request_data})
        if not response.ok:
            print(response.json())
            raise RequestException
        return response.json()

    def _assign_api_endpoint(self):
        env = self._sa_instance_state.session.info["env"]
        if env == "production":
            return PRODUCTION
        return STAGING

    def _parse_date_to_iso(self, date):
        return date.isoformat() + "Z"

    def _format_bools(self, mutation):
        return mutation.replace('"true"', "true").replace('"false"', "false")

    def _validate_backfill_arg(self, **kwargs):
        backfill_date = kwargs.get("backfill_datetime", None)
        if backfill_date and not isinstance(backfill_date, datetime.datetime):
            raise TypeError(
                """backfill_date must be of type
                             datetime.datetime, received type {}
                            """.format(
                    type(backfill_date)
                )
            )

    def _validate_workflow_args(self, **kwargs):
        self._validate_passed_fields(**kwargs)
        self._validate_backfill_arg(**kwargs)

    def _translate_workflow_args(self, **kwargs):
        data = {}
        ref = {
            "enabled": "isWorkflowEnabled",
            "trained": "isWorkflowTrained",
            "backfill_datetime": "backfillDateTime",
            "trait_id": "traitIds",
        }
        for k, v in kwargs.items():
            reformatted = ref[k]
            if type(v) is bool:
                v = str(v).lower()
            if type(v) is datetime.datetime:
                v = self._parse_date_to_iso(v)
            data[reformatted] = v
        return data

    def _set_user(self, request, mutation_type):
        user = getpass.getuser()
        payload = {"workflowId": str(self.id)}
        if mutation_type == SkittlesMixin.CREATE:
            payload["createdBy"] = user
        else:
            payload["updatedBy"] = user
        return payload

    def _format_mutation_request(self, mutation_type, **kwargs):
        translated = self._translate_workflow_args(**kwargs)
        with_user = self._set_user(translated, mutation_type)
        with_user.update(translated)
        return {k: '"{}"'.format(str(v)) for k, v in with_user.items()}

    def _format_query_request(self):
        formatted_id = '"{}"'.format(str(self.id))
        return SkittlesMixin.GET_WORKFLOW.format(
            workflow_id=formatted_id, schema=SkittlesMixin.SCHEMA
        )

    def _parse_mutation(self, mutation_type, **kwargs):
        request = self._format_mutation_request(mutation_type, **kwargs)
        mutation = mutation_type.format(request=request, schema=SkittlesMixin.SCHEMA)
        formatted_mutation = self._format_bools(mutation)
        return SkittlesMixin.MUTATION.format(mutation=formatted_mutation)

    def _parse_get_query(self):
        request = self._format_query_request()
        return SkittlesMixin.QUERY.format(query=request)

    def __update_workflow(self, **kwargs):
        mutation = self._parse_mutation(SkittlesMixin.UPDATE, **kwargs)
        return self.__post_to_graphql(mutation)

    def __create_workflow(self, **kwargs):
        mutation = self._parse_mutation(SkittlesMixin.CREATE, **kwargs)
        return self.__post_to_graphql(mutation)

    def query_skittles(self):
        query = self._parse_get_query()
        return self.__post_to_graphql(query)

    def update_skittles_workflow(self, **kwargs):
        self._validate_workflow_args(**kwargs)
        return self.__update_workflow(**kwargs)

    def commit_to_skittles(self, trait_id, backfill_datetime):
        if not isinstance(backfill_datetime, datetime.datetime):
            raise TypeError(
                """A datetime.datetime object is
                            required to commit a workflow to Skittles,
                            received argument of type {}
                            """.format(
                    type(backfill_datetime)
                )
            )
        return self.__create_workflow(
            trait_id=trait_id,
            backfill_datetime=backfill_datetime,
            enabled=True,
            trained=True,
        )

    def skittles_enable(self):
        return self.__update_workflow(enabled=True)

    def skittles_disable(self):
        return self.__update_workflow(enabled=False)

    def toggle_skittles_status(self, enabled):
        if not isinstance(enabled, bool):
            raise TypeError(
                """Toggling a workflow's status requires
                            passing a boolean value, received {}
                            """.format(
                    type(enabled)
                )
            )
        return self.__update_workflow(enabled=True)
