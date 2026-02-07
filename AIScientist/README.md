# AI Co-Scientist (AIScientist)

An agentic AI co-scientist application that connects to UniProt to support pre-clinical scientists with protein discovery, hypothesis generation, and task planning.

## Features
- **UniProt integration** via the REST API for protein search and entry retrieval.
- **Agentic responses** that synthesize functional annotations into hypotheses, interpretation cues, and next-step tasks.
- **User-friendly interface** for rapid exploration and iteration.

## Local development

### Backend + frontend (single service)
The FastAPI backend serves the frontend assets for a simple local MVP.

```bash
cd AIScientist
python -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
uvicorn backend.main:app --reload --port 8000
```

Open <http://localhost:8000> in your browser.

### API quick test
```bash
curl "http://localhost:8000/api/search?query=EGFR"
```

## Cloud-ready deployment
A Dockerfile is included for containerized deployment on platforms like Azure Container Apps, AWS ECS, or GCP Cloud Run.

```bash
docker build -t aiscientist:latest .
docker run -p 8000:8000 aiscientist:latest
```

## Project structure
```
AIScientist/
  backend/         # FastAPI service + UniProt client
  frontend/        # HTML/CSS/JS UI
  Dockerfile       # Container entry
```

## Notes
- UniProt REST API documentation: <https://rest.uniprot.org/>
- Extend the `analyze` endpoint to add custom reasoning logic or model integrations.
