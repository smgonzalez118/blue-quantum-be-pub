from rest_framework.routers import DefaultRouter
from .views import DashboardViewSet, FavoriteViewSet, favoritos_detalle
from django.urls import path


router = DefaultRouter()
router.register(r'', DashboardViewSet, basename = "dashboard")
router.register(r'favorites', FavoriteViewSet, basename="favorites")

urlpatterns = router.urls

urlpatterns += [path("favoritos/detalle/", favoritos_detalle, name="favoritos_detalle")]