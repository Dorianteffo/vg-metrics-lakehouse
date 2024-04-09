# Overview 


## Schema Summary:

Game_Info
* GameID (string): Unique identifier for the game.
* Genre (string): Genre of the game (e.g., MMO, FPS, RPG, Adventure, Strategy).
* Publisher (string): Publisher of the game.
* Rating (string): ESRB rating of the game (e.g., 'E', 'T', 'M').
* Game_Length (int): Length of the game in hours.
* ReleaseDate (date): When the game was first released.


Player_Activity
* PlayerID (string): Unique identifier for each player.
* GameID (string): Identifier for the game the player is playing.
* SessionID (string): Unique identifier for the player's gaming session.
* StartTime (datetime): When the player started their current session.
* EndTime (datetime, nullable): When the player ended their current session, if applicable.
* ActivityType (string): Type of activity (e.g., Playing, AFK, In-Queue).
* Level (int): The player's game level during this session.
* ExperiencePoints (float): Experience points earned during this session.
* AchievementsUnlocked (int): Number of achievements unlocked during this session.
* CurrencyEarned (float): In-game currency or points earned during the session.
* CurrencySpent (float): In-game currency or points spent during the session.
* QuestsCompleted (int): Number of quests or missions completed during the session.
* EnemiesDefeated (int): Number of enemies or opponents defeated during the session.
* ItemsCollected (int): Number of items collected during the session.
* Deaths (int): Number of times the player died during the session.
* DistanceTraveled (float): Distance traveled in the game world during the session.
* ChatMessagesSent (int): Number of chat messages sent during the session.
* TeamEventsParticipated (int): Number of team events or multiplayer activities participated in.
* SkillLevelUp (int): Number of times any skill was leveled up during the session.
* PlayMode (string): Type of play mode engaged in (e.g., Solo, Co-op, PvP).
* SessionDuration (float): Total duration of the session in minutes.