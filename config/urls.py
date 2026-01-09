from django.contrib import admin
from django.urls import path, include


urlpatterns = [
    path("admin/", admin.site.urls),
    path('api/usuarios/', include('usuarios.urls')),
    path('api/senales/', include('senales.urls')),
    path('api/dashboard/', include('dashboard.urls')),
    path('api/activo/', include('activo.urls')),
    path('api/portafolio/', include('portafolio.urls')),
    path('api/forecasting/', include('forecasting.api.urls')),
    path('', include('market.urls')),
]
