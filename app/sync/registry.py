from __future__ import annotations

from typing import Dict, List

from app.sync.base import SyncHandler
from app.sync.farmer_groups import FarmerGroupsHandler
from app.sync.farmers import FarmersHandler
from app.sync.households import HouseholdsHandler
from app.sync.project_staff_roles import ProjectStaffRolesHandler
from app.sync.training_sessions import TrainingSessionsHandler

# Registry of all supported entities to be sent to CommCare.
ORDERED_ENTITIES: List[str] = [
    "project_staff_roles",
    "farmer_groups",
    "training_sessions",
    "households",
    "farmers",
    # "wetmills",,
]


# Mapping of entity names to their respective SyncHandler instances.
HANDLERS: Dict[str, SyncHandler] = {
    "project_staff_roles": ProjectStaffRolesHandler(),
    "farmer_groups": FarmerGroupsHandler(),
    "training_sessions": TrainingSessionsHandler(),
    "households": HouseholdsHandler(),
    "farmers": FarmersHandler(),
}
