from cattledb.restserver import create_app_by_config

app = create_app_by_config()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=False)