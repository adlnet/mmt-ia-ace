from api import views
from api.views import WorkflowView
from django.urls import path
from rest_framework.routers import DefaultRouter

router = DefaultRouter()

app_name = 'api'

urlpatterns = [
    path('xia-workflow/', WorkflowView.as_view(), name='xia_workflow'),
    path('credit-data/', views.CreditDataView.as_view(),
         name='credit-data'),
]
