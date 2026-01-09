from rest_framework import serializers
from .models import CompanyProfile, FundamentalMetric, ReporteTecnico

class ReporteTecnicoSerializer(serializers.ModelSerializer):
    
    class Meta:
        model = ReporteTecnico
        fields = '__all__'


class CompanyProfileSerializer(serializers.ModelSerializer):
    ticker = serializers.CharField(source="ticker.symbol", read_only=True)

    class Meta:
        model = CompanyProfile
        fields = [
            "ticker", "name", "country", "exchange", "currency",
            "sector", "industry", "market_cap", "shares_outstanding", "updated_at"
        ]

class FundamentalMetricSerializer(serializers.ModelSerializer):
    ticker = serializers.CharField(source="ticker.symbol", read_only=True)

    class Meta:
        model = FundamentalMetric
        fields = ["ticker", "metric", "period_end", "value"]