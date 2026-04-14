# UptimeRobot Keep-Alive

Use UptimeRobot to visit the deployed Quote Tool before Streamlit Community Cloud hibernates it.

## Monitor

- Monitor type: HTTP(s)
- Friendly name: DGA Quote Tool
- URL to monitor: `https://dga-quote-tool-v5.streamlit.app/?health=1`
- Monitoring interval: 6 hours
- Expected status: 200 OK
- Optional keyword check: `UPTIME_OK`

The `?health=1` URL renders a lightweight page that confirms the app process is awake without loading the full quoting workflow.

## Team Recovery

If the app still shows Streamlit's sleeping page, click **Yes, get this app back up!**. Anyone with access to the app can wake it; it does not have to be the app owner.

This repo also includes a scheduled GitHub Actions fallback in `.github/workflows/keep-streamlit-awake.yml`. That job opens the health URL with a browser every 6 hours and clicks Streamlit's wake button if the app has already hibernated.
