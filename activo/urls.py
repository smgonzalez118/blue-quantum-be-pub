from django.urls import path
from rest_framework.routers import DefaultRouter
from . import views

router = DefaultRouter()
router.register(r'reporte_tecnico', views.ReporteTecnicoViewSet, basename='reporte_tecnico')

urlpatterns = [
    path('info/<str:ticker>/', views.info_empresa),
    path('comparables/<str:ticker>/', views.comparables),
    path('cambios/<str:ticker>/', views.cambios_recientes),
    path('precio_evo/<str:ticker>/<str:timeframe>/', views.precio_evo),
    path('comparativo/<str:ticker>/<str:timeframe>/', views.comparativo_normalizado),
    path('volatilidad/<str:ticker>/<str:timeframe>/', views.volatilidad),
    path("fundamentals/<str:ticker>/", views.fundamentals, name="fundamentals")

]

urlpatterns += router.urls
