from rest_framework import serializers

class ForecastRequestSerializer(serializers.Serializer):
    ticker = serializers.CharField()

class ForecastHorizonSerializer(serializers.Serializer):
    key = serializers.CharField()
    days = serializers.IntegerField()
    price_now = serializers.FloatField(allow_null=True)
    price_pred = serializers.FloatField(allow_null=True)
    ret_pct_pred = serializers.FloatField(allow_null=True)

class ForecastResponseSerializer(serializers.Serializer):
    ticker = serializers.CharField()
    horizons = ForecastHorizonSerializer(many=True)
