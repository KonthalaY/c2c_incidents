## IncidentData Table

This table stores information about incidents.

| Column         | Type     | Description                 |
| -------------- | -------- | --------------------------- |
| uid            | Integer  | Unique identifier           |
| id             | String   | Incident ID                 |
| desc           | String   | Description of the incident |
| roadway        | String   | Roadway affected            |
| direction      | String   | Direction of the incident   |
| crossstreet    | String   | Cross street                |
| lat            | String   | Latitude                    |
| lon            | String   | Longitude                   |
| status         | String   | Incident status             |
| updateType     | String   | Type of update              |
| severity       | String   | Severity level              |
| eventType      | String   | Type of event               |
| confirmedDate  | DateTime | Date of confirmation        |
| confirmedTime  | Time     | Time of confirmation        |
| timestamp      | DateTime | Timestamp                   |

## IncidentLaneDetail Table

This table stores information about lane details associated with incidents.

| Column	| Type	   | Description             |
| --------- | -------- | ----------------------- |
| uid	    | Integer  | Unique identifier       |
| id	    | String   | Incident ID             |
| typ	    | String   | Type of lane            |
| status	| String   | Lane status             |
| index	    | Integer  | Index value of the lane |

## IncidentAffectedLane Table

This table stores information about affected lanes for incidents.

| Column | Type	   | Description                         |
| ------ | ------- | ----------------------------------- |
| uid	 | Integer | Unique identifier                   |
| id     | String  | Incident ID                         |
| key	 | String  | Key associated with affected lane   |
| value  | String  | Value associated with affected lane |