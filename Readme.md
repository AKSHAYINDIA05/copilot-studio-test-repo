We are building Phase 1 of an AI learning platform.

Do not build the full application yet. Build only the local working MVP for roadmap generation and storage.

## Phase 1 scope

A user should be able to:

1. Sign up
2. Log in
3. Enter a learning prompt
4. Generate a structured roadmap using an LLM
5. Store the roadmap, topics, and subtopics in PostgreSQL
6. View all generated roadmaps on dashboard
7. Open a roadmap detail page and see ordered topics/subtopics

Do not implement topic content generation.
Do not implement quizzes.
Do not implement progress tracking.
Do not implement JD upload.
Do not implement learning cards.
Do not implement profile analytics.

## Tech stack

Frontend:

* Next.js
* TypeScript
* Tailwind CSS
* shadcn/ui

Backend:

* FastAPI
* Python 3.11+
* Pydantic
* SQLAlchemy async
* Alembic

Database:

* PostgreSQL

Cache:

* Valkey in Docker Compose, but optional usage in Phase 1

Local development:

* Docker Compose

## Required repo structure

Create this structure:

ai-learning-platform/
backend/
frontend/
infra/
docs/
docker-compose.yml
README.md

## Backend requirements

Create a clean FastAPI backend with:

* versioned APIs
* health endpoint
* structured config
* async PostgreSQL connection
* Alembic migrations
* JWT-based local auth
* password hashing
* SQLAlchemy models
* Pydantic schemas
* repository/service separation
* structured logging middleware
* request ID middleware
* `.env.example`

## Backend database tables for Phase 1

Create only:

* users
* roadmaps
* roadmap_topics
* roadmap_subtopics
* activity_logs

## Backend endpoints

Implement:

Health:

* GET `/health`

Auth:

* POST `/api/v1/auth/signup`
* POST `/api/v1/auth/login`
* GET `/api/v1/auth/me`

Roadmaps:

* POST `/api/v1/roadmaps/generate`
* GET `/api/v1/roadmaps`
* GET `/api/v1/roadmaps/{roadmap_id}`

## Roadmap generation behavior

`POST /api/v1/roadmaps/generate` should:

1. Accept a prompt:
   {
   "prompt": "I want to learn Python at beginner level"
   }

2. Authenticate the user.

3. Call an LLM service abstraction:
   generate_roadmap(prompt: str) -> RoadmapLLMResponse

4. For local development, make the LLM provider configurable:

   * If `LLM_PROVIDER=mock`, return a realistic mock roadmap.
   * If `LLM_PROVIDER=openai`, call OpenAI-compatible API using env variables.

5. Validate LLM output using Pydantic.

6. Store:

   * roadmap
   * topics
   * subtopics

7. Return saved roadmap response.

Important:

* Do not generate full topic content.
* Do not generate quiz questions.
* Roadmap must only include metadata, topics, and subtopics.

## Roadmap schema

Use this schema for the LLM response and API response:

{
"roadmapTitle": "",
"roadmapDescription": "",
"roadmapType": "learning_prompt",
"goal": "",
"targetAudience": "",
"targetLevel": "beginner",
"estimatedTotalTime": "",
"recommendedWeeklyHours": "",
"successOutcome": "",
"generatedFrom": {
"type": "learning_prompt",
"originalPrompt": "",
"jobDescriptionMode": false,
"jobTitle": "",
"companyName": "",
"interviewDate": "",
"techStackFocus": [],
"extractedSkills": []
},
"topics": [
{
"topicOrder": 1,
"topicName": "",
"topicDescription": "",
"difficulty": "beginner",
"estimatedTimeToComplete": "",
"importance": "high",
"learningObjectives": [],
"prerequisites": [],
"skillsCovered": [],
"completionCriteria": [],
"subtopics": [
{
"subTopicOrder": 1,
"subTopicName": "",
"subTopicDescription": "",
"difficulty": "beginner",
"estimatedTimeToComplete": "",
"importance": "high",
"learningObjectives": [],
"prerequisites": [],
"skillsCovered": [],
"completionCriteria": []
}
]
}
]
}

## Frontend requirements

Create a clean Next.js frontend with:

Pages:

* landing page
* signup page
* login page
* dashboard page
* generate roadmap page
* roadmap detail page

Frontend behavior:

* User can sign up and log in
* Store JWT locally for Phase 1
* User can submit a learning prompt
* Show loading state while roadmap is generated
* Show generated roadmap detail page
* Dashboard lists saved roadmaps
* Roadmap detail page displays topics and subtopics in expandable sections

UI requirements:

* Use Tailwind CSS
* Use shadcn/ui components
* Clean modern design
* Handle loading, empty, and error states
* Use a typed API client

## Docker Compose

Create local Docker Compose with:

* backend
* frontend
* postgres
* valkey

Postgres should persist data using a volume.

## Deliverables

Generate:

1. Full folder structure
2. All backend code
3. Alembic migration
4. All frontend code
5. Docker Compose
6. `.env.example`
7. README with local run instructions

Do not skip code.
Do not describe only high-level architecture.
Generate runnable Phase 1 implementation.
