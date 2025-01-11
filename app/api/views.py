import logging
import xml.etree.ElementTree as element_Tree

import pandas as pd
from celery.result import AsyncResult
from core.management.commands.extract_source_metadata import \
    get_source_metadata
from core.tasks import execute_xia_automated_workflow
from django.http import JsonResponse
from rest_framework import permissions, status
from rest_framework.decorators import permission_classes
from rest_framework.parsers import BaseParser
from rest_framework.response import Response
from rest_framework.views import APIView

logger = logging.getLogger('dict_config_logger')


class PlainTextParser(BaseParser):
    """
    Plain text parser.
    """
    media_type = 'application/xml'

    def parse(self, stream, media_type=None, parser_context=None):
        """
        Simply return a string representing the body of the request.
        """
        return stream.read()


@permission_classes((permissions.AllowAny,))
class WorkflowView(APIView):
    """Handles HTTP requests for Metadata for XIS"""

    def get(self, request):
        logger.info('XIA workflow api')
        task = execute_xia_automated_workflow.delay()
        response_val = {"task_id": task.id}

        return Response(response_val, status=status.HTTP_202_ACCEPTED)


def get_status(request, task_id):
    task_result = AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return JsonResponse(result, status=200)


@permission_classes((permissions.AllowAny,))
class CreditDataView(APIView):
    """Handles HTTP requests for Credential data for XIA"""
    parser_classes = [PlainTextParser]

    def post(self, request):

        logger.info("request.data")
        logger.info(request.data)

        xsr_root = element_Tree.fromstring(request.data)

        xsr_items = []

        for s_item in xsr_root.findall('.//version'):
            xsr_items.append(s_item.attrib)

            for count, item in enumerate(s_item):

                xsr_items[-1][item.tag] = {}
                xsr_items[-1][item.tag] = item.attrib

                for count1, c_item in enumerate(item):

                    if c_item.tag not in xsr_items[-1][item.tag]:
                        xsr_items[-1][item.tag][c_item.tag] = list()

                    if c_item.text:
                        xsr_items[-1][item.tag][c_item.tag].append(
                            {c_item.tag: c_item.text})
                        (xsr_items[-1][item.tag][c_item.tag][-1]).update(
                            c_item.attrib)
                    else:
                        xsr_items[-1][item.tag][c_item.tag].append(
                            c_item.attrib)

                    for count1_1, item1_1 in enumerate(c_item):

                        (xsr_items[-1][item.tag][c_item.tag]
                         [-1][item1_1.tag]) = (
                            item1_1.attrib)

                        for count2, cs_item in enumerate(item1_1):

                            if cs_item.tag not in (xsr_items[-1][item.tag]
                                                   [c_item.tag][-1]
                                                   [item1_1.tag]):
                                (xsr_items[-1][item.tag][c_item.tag][-1]
                                 [item1_1.tag][cs_item.tag]) = list()

                            if cs_item.text:
                                (xsr_items[-1][item.tag][c_item.tag]
                                 [-1][item1_1.tag][cs_item.tag]). \
                                    append({cs_item.tag: cs_item.text})
                                (xsr_items[-1][item.tag][c_item.tag]
                                 [-1][item1_1.tag][cs_item.tag][-1]). \
                                    update(cs_item.attrib)
                            else:
                                (xsr_items[-1][item.tag][c_item.tag]
                                 [-1][item1_1.tag][cs_item.tag]).append(
                                    cs_item.attrib)

        source_df = pd.DataFrame(xsr_items)
        std_source_df = source_df.where(pd.notnull(source_df),
                                        None)
        get_source_metadata([std_source_df])

        response_text = {"message": "Extraction of data successful!"}

        return Response(response_text, status=status.HTTP_200_OK)
