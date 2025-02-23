from crewai import Agent, Crew, Process, Task
from crewai.project import CrewBase, agent, crew, task
from dotenv import load_dotenv
import os
from main_crew.src.main_crew.tools.postgres import PostgresTool
from pydantic import BaseModel, Field
from crewai.llm import LLM
load_dotenv()
api_key = os.getenv("API_KEY")
model = os.getenv("MODEL")

# Instantiate the Postgres tool
pg_tool = PostgresTool(
    db_name="cms-teable",
    user="postgres",
    password="7tgXNnfA52tsbjd9FvrIt8yPJlphtvebVp5qk6PFj5c1hEC5eB8Cyoy9PNhjGbru",
    host="94.130.161.59",
    port=5543,
    table_schema="bse5uGrG2eJswURuaGL"
)
class CrewResponse(BaseModel):
    status: str = Field(..., description="Indicates the operation status, e.g., 'success' or 'error'.")
    data: dict = Field(..., description="Holds the payload data returned from the crew's tasks.")
    message: str = Field(None, description="Optional message providing additional context or details.")

@CrewBase
class MainCrew():
    """MainCrew crew"""
    agents_config = 'config/agents.yaml'
    tasks_config = 'config/tasks.yaml'

    @agent
    def customer_service_agent(self) -> Agent:
        return Agent(
            config=self.agents_config['customer_service_agent'],
            verbose=True,
            tools=[pg_tool],
            llm=LLM(model="o3-mini")
        )

    @task
    def respond_to_question(self) -> Task:
        return Task(
            config=self.tasks_config['respond_to_question'],
            output_pydantic=CrewResponse
        )
    
    @crew
    def crew(self) -> Crew:
        """Creates the MainCrew crew"""
        return Crew(
            agents=self.agents,
            tasks=self.tasks,
            process=Process.sequential,
            verbose=True,
        )
