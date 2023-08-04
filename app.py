from flask import Flask, jsonify, Response, request
from database import get_connection
from report_generator import generate_report, is_report_generation_complete, get_report_data, create_csv_from_report

app = Flask(__name__)

# API endpoints
@app.route('/trigger_report', methods=['POST'])
def trigger_report():
    connection = get_connection()
    report_id = generate_report(connection)
    connection.close()
    return jsonify({'report_id': report_id})

@app.route('/get_report', methods=['GET'])
def get_report():
    report_id = request.args.get('report_id')
    connection = get_connection()
    if is_report_generation_complete(report_id,connection):
        report_data = get_report_data(report_id, connection)
        if report_data:
            csv_data = create_csv_from_report(report_data)
            connection.close()
            return Response(csv_data, mimetype='text/csv', headers={'Content-Disposition': 'attachment; filename=report.csv'})
    connection.close()
    return jsonify({'status': 'Running'})

if __name__ == '__main__':
    app.run()
