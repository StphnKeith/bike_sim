"""Assignment 1 - Bike-share objects

=== CSC148 Fall 2017 ===
Diane Horton and David Liu
Department of Computer Science,
University of Toronto


=== Module Description ===

This file contains the Station and Ride classes, which store the data for the
objects in this simulation.

There is also an abstract Drawable class that is the superclass for both
Station and Ride. It enables the simulation to visualize these objects in
a graphical window.
"""
from datetime import datetime
from typing import Tuple


# Sprite files
STATION_SPRITE = 'stationsprite.png'
RIDE_SPRITE = 'bikesprite.png'


class Drawable:
    """A base class for objects that the graphical renderer can be drawn.

    === Public Attributes ===
    sprite:
        The filename of the image to be drawn for this object.
    """
    sprite: str

    def __init__(self, sprite_file: str) -> None:
        """Initialize this drawable object with the given sprite file.
        """
        self.sprite = sprite_file

    def get_position(self, time: datetime) -> Tuple[float, float]:
        """Return the (long, lat) position of this object at the given time.
        """
        raise NotImplementedError


class Station(Drawable):
    """A Bixi station.

    === Public Attributes ===
    capacity:
        the total number of bikes the station can store
    location:
        the location of the station in long/lat coordinates
        **UPDATED**: make sure the first coordinate is the longitude,
        and the second coordinate is the latitude.
    name: str
        name of the station
    num_bikes: int
        current number of bikes at the station
    num_rides_started:
        The number of rides started at this station during the simulation.
    num_rides_ended:
        The number of rides ended at this station during the simulation.
    time_low_availability:
        The time in seconds that this station had no more than 5 bikes available
        during the simulation.
    time_low_unoccupied:
        The time in seconds that this station had no more than 5 unoccupied
        spots during the simulation.
    === Representation Invariants ===
    - 0 <= num_bikes <= capacity
    """
    name: str
    location: Tuple[float, float]
    capacity: int
    num_bikes: int
    num_rides_started: int
    num_rides_ended: int
    time_low_availability: int
    time_low_unoccupied: int

    def __init__(self, pos: Tuple[float, float], cap: int,
                 num_bikes: int, name: str) -> None:
        """Initialize a new station.

        Precondition: 0 <= num_bikes <= cap
        """
        self.location = pos
        self.capacity = cap
        self.num_bikes = num_bikes
        self.name = name
        self.num_rides_started = 0
        self.num_rides_ended = 0
        self.time_low_availability = 0
        self.time_low_unoccupied = 0
        Drawable.__init__(self, STATION_SPRITE)

    def get_position(self, time: datetime) -> Tuple[float, float]:
        """Return the (long, lat) position of this station for the given time.

        Note that the station's location does *not* change over time.
        The <time> parameter is included only because we should not change
        the header of an overridden method.
        """
        return self.location


class Ride(Drawable):
    """A ride using a Bixi bike.

    === Attributes ===
    start:
        the station where this ride starts
    end:
        the station where this ride ends
    start_time:
        the time this ride starts
    end_time:
        the time this ride ends

    === Representation Invariants ===
    - start_time < end_time
    """
    start: Station
    end: Station
    start_time: datetime
    end_time: datetime

    def __init__(self, start: Station, end: Station,
                 times: Tuple[datetime, datetime]) -> None:
        """Initialize a ride object with the given start and end information.
        """
        self.start, self.end = start, end
        self.start_time, self.end_time = times[0], times[1]
        Drawable.__init__(self, RIDE_SPRITE)

    def get_position(self, time: datetime) -> Tuple[float, float]:
        """Return the (long, lat) position of this ride for the given time.

        A ride travels in a straight line between its start and end stations
        at a constant speed.
        """
        progress = (time - self.start_time) / (self.end_time - self.start_time)
        x_progress = (self.end.location[0] - self.start.location[0]) * progress
        y_progress = (self.end.location[1] - self.start.location[1]) * progress
        x_position = self.start.location[0] + x_progress
        y_position = self.start.location[1] + y_progress
        return (x_position, y_position)


if __name__ == '__main__':
    import python_ta
    python_ta.check_all(config={
        'allowed-import-modules': [
            'doctest', 'python_ta', 'typing',
            'datetime'
        ],
        'max-attributes': 15
    })
