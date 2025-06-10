# SCP (Subunit-Channel-Position) Identifier in MTA Turnstile Data

## Overview

The **SCP** code is a unique identifier for each turnstile device in the MTA subway system. It serves as an "address" within the station's fare control system. Each SCP follows the format `XX-YY-ZZ` (e.g., `00-00-00` or `01-00-01`), representing three distinct components in a hierarchical structure.

## Components

### 1. Subunit (First Two Digits)

- Designates a **sub-unit** of the station's fare control hardware
- Represents a controller or module that manages a group of turnstiles within a station entrance (control area)
- Each control area may have one or more subunits depending on turnstile count
- Typically numbered sequentially: `00`, `01`, `02`, etc.
- Example: `00` might indicate the primary controller for the main bank of turnstiles

### 2. Channel (Middle Two Digits)

- Represents a **communication line or port** on the subunit controller
- Each subunit can have multiple channels connected to different turnstiles
- Channels are typically numbered starting at `00` for each subunit
- Simple setups often use only channel `00` for all turnstiles
- Busier stations may utilize multiple channels (`01`, `02`, `03`, etc.)
- Channel numbers may not appear sequential due to installation configurations

### 3. Position (Last Two Digits)

- Identifies the **specific turnstile** connected to that channel
- Distinguishes individual turnstile units when multiple are connected to the same channel
- Positions are typically numbered sequentially (`00`, `01`, `02`, etc.) for each channel
- For example, in `01-00-03`, "03" identifies the fourth turnstile on subunit 01's channel 00

## Assignment Logic

The SCP values follow these principles:

- **Uniqueness**: Each complete SCP code is unique within a given control area and remote unit
- **Hierarchical**: The three-part address follows the physical/logical hierarchy of the fare control system
- **Local Numbering**: The numbering resets per station or control area; multiple stations might have a turnstile "00-00-00"
- **Physical Layout**: SCP groupings often reflect physical layout; turnstiles controlled by the same booth controller share the subunit number
- **Installation-based**: SCPs are assigned during equipment installation by technicians
- **No Global Pattern**: There is no system-wide meaning for specific numbers; they are assigned based on the station's configuration

## Practical Examples

### Simple Station Entrance (5 turnstiles)
- All turnstiles might use subunit `00`, channel `00`
- Individual turnstiles: `00-00-00`, `00-00-01`, `00-00-02`, `00-00-03`, `00-00-04`

### Station with Two Entrances
- First entrance turnstiles: `00-00-00`, `00-00-01`, etc. (under subunit `00`)
- Second entrance turnstiles: `01-00-00`, `01-00-01`, etc. (under subunit `01`)

### Complex Station (like 34th St – Penn Station)
- One bank of turnstiles: `01-00-02`, `01-00-03`, ..., `01-00-05`
- Another bank: `00-00-00`, `00-00-01`, ..., `00-00-04`

## Summary

The SCP code functions as a technical addressing system within the MTA's fare control infrastructure. While the format is consistent (always three two-digit parts), the assigned values are specific to each station's configuration. The SCP code, combined with the Control Area (C/A) and Unit identifiers, creates a globally unique identifier for each turnstile in the NYC subway system, allowing the MTA to track entry and exit counts for each specific device.