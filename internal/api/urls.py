from rest_framework.routers import DefaultRouter
from django.urls import path, include


from . import viewsets

router = DefaultRouter()
router.register("datasource", viewsets.DataSourceViewSet, basename="datasource")

router_patterns = {
    path(
        "query/<str:data_source>/",
        include(
            [
                path(
                    "select/",
                    viewsets.QueryViewSet.as_view({"get": "select"}),
                ),
            ]
        ),
    )
}

urlpatterns = [
    *router_patterns,
    *router.urls,
]
