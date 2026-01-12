from pydantic import BaseModel, Field

class JustifyBody(BaseModel):
    agent_id: str = Field(..., description="Agent ID")
    date: str = Field(..., description="YYYY-MM-DD")
    type: str = Field(..., pattern=r"^(A|J|V|U|D|H|C)$")
    note: str = Field(default="", description="Optional note")
    lead: str = Field(default="", description="Optional lead")