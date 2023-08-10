
# Configurar alarme para monitorar o tempo de execução
cloudwatch.put_metric_alarm(
    AlarmName='ETLExecutionTimeAlarm',
    AlarmDescription='Alerta de tempo de execução do ETL',
    MetricName='ETLExecutionTime',
    Namespace='ETLProcess',
    Statistic='Average',
    ComparisonOperator='GreaterThanThreshold',
    Threshold=300,  # Definir o limite em segundos
    Period=300,     # Intervalo de verificação em segundos
    EvaluationPeriods=1,
    AlarmActions=['arn:aws:sns:us-east-1:123456789012:MySnsTopic']
)
