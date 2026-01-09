from rest_framework.routers import DefaultRouter
from .views import SenalTecnicaViewSet

router = DefaultRouter()
router.register(r'', SenalTecnicaViewSet, basename = "senal")

urlpatterns = router.urls