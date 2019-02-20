"""Assignment 1 - Simulation

=== CSC148 Fall 2017 ===
Diane Horton and David Liu
Department of Computer Science,
University of Toronto


=== Module Description ===

This file contains the Simulation class, which is the main class for your
bike-share simulation.

At the bottom of the file, there is a sample_simulation function that you
can use to try running the simulation at any time.
"""
import csv
from datetime import datetime, timedelta
import json
from typing import Dict, List, Tuple

from bikeshare import Ride, Station
from container import PriorityQueue
from visualizer import Visualizer

# Datetime format to parse the ride data
DATETIME_FORMAT = '%Y-%m-%d %H:%M'


class Simulation:
    """Runs the core of the simulation through time.

    === Attributes ===
    all_rides:
        A list of all the rides in this simulation.
        Note that not all rides might be used, depending on the timeframe
        when the simulation is run.
    all_stations:
        A dictionary containing all the stations in this simulation.
    active_rides:
        A list containing all the rides currently in progress.
    event_queue:
        A priority queue containing all the events to be processed by the
        simulation. Note that some events added during the run of the
        simulation may not be processed, as they fall outside of the run time.
    visualizer:
        A helper class for visualizing the simulation.
    """
    all_stations: Dict[str, Station]
    all_rides: List[Ride]
    active_rides: List[Ride]
    event_queue: PriorityQueue['Event']
    visualizer: Visualizer

    def __init__(self, station_file: str, ride_file: str) -> None:
        """Initialize this simulation with the given configuration settings.
        """
        self.all_stations = create_stations(station_file)
        self.all_rides = create_rides(ride_file, self.all_stations)
        self.active_rides = []
        self.event_queue = PriorityQueue()
        self.visualizer = Visualizer()

    def run(self, start: datetime, end: datetime) -> None:
        """Run the simulation from <start> to <end>.
        """
        step = timedelta(minutes=1)

        # Add ride start events to event_queue
        for ride in self.all_rides:
            if ride.start_time <= end:
                ride_start = RideStartEvent(self, ride.start_time, ride)
                self.event_queue.add(ride_start)

        # Begin main loop
        while start <= end:
            # Update active_rides and render sprites
            self._update_active_rides_fast(start)
            drawables = self.active_rides + list(self.all_stations.values())
            self.visualizer.render_drawables(drawables, start)

            # Update statistics stored in the Station objects. Don't update
            # time related statistics if it's the last minute of the simulation
            self._update_ride_statistics(start)
            if start != end:
                self._update_time_statistics()

            start += step

        while True:
            if self.visualizer.handle_window_events():
                return

    def _update_ride_statistics(self, current_time: datetime) -> None:
        """Updates the number of rides started and ended at each station.

        Also updates the number of bikes available at each station.
        """
        for ride in self.active_rides:
            if current_time == ride.start_time:
                ride.start.num_rides_started += 1
                ride.start.num_bikes -= 1

            if current_time == ride.end_time:
                if ride.end.num_bikes < ride.end.capacity:
                    ride.end.num_rides_ended += 1
                    ride.end.num_bikes += 1

    def _update_time_statistics(self) -> None:
        """Updates the time spent at low availability and low occupied
        for each station.

        As the time increments this method adds 60 seconds to a station's
        time_low_availability and time_low_unoccupied attributes if it
        has no more than 5 bikes and no more than 5 open spots respectively.
        """
        for station in self.all_stations.values():
            if station.num_bikes <= 5:
                station.time_low_availability += 60
            if station.capacity - station.num_bikes <= 5:
                station.time_low_unoccupied += 60

    def _update_active_rides(self, time: datetime) -> None:
        """Update this simulation's list of active rides for the given time.

        REQUIRED IMPLEMENTATION NOTES:
        -   Loop through `self.all_rides` and compare each Ride's start and
            end times with <time>.

            If <time> is between the ride's start and end times (inclusive),
            then add the ride to self.active_rides if it isn't already in
            that list.

            Otherwise, remove the ride from self.active_rides if it is in
            that list.

        -   This means that if a ride started before the simulation's time
            period but ends during or after the simulation's time period,
            it should still be added to self.active_rides.
        """
        for ride in self.all_rides:
            if ride.start_time <= time <= ride.end_time:
                if ride not in self.active_rides and ride.start.num_bikes > 0:
                    self.active_rides.append(ride)
            elif ride in self.active_rides:
                self.active_rides.remove(ride)

    def calculate_statistics(self) -> Dict[str, Tuple[str, float]]:
        """Return a dictionary containing statistics for this simulation.

        The returned dictionary has exactly four keys, corresponding
        to the four statistics tracked for each station:
          - 'max_start'
          - 'max_end'
          - 'max_time_low_availability'
          - 'max_time_low_unoccupied'

        The corresponding value of each key is a tuple of two elements,
        where the first element is the name (NOT id) of the station that has
        the maximum value of the quantity specified by that key,
        and the second element is the value of that quantity.

        For example, the value corresponding to key 'max_start' should be the
        name of the station with the most number of rides started at that
        station, and the number of rides that started at that station.

        Student Note: This function makes a list of stats for each station and
        then loops through the dictionary and the list simultaneously and
        compares each item with each value in the dictionary. This is done to
        reduce duplicate code.
        """
        stats_dict = {
            'max_start': ['', -1],
            'max_end': ['', -1],
            'max_time_low_availability': ['', -1],
            'max_time_low_unoccupied': ['', -1]
        }

        # Loop through each station
        for station in self.all_stations.values():
            # Store the stats for each station in a list to be iterated over
            station_stats = [station.num_rides_started, station.num_rides_ended,
                             station.time_low_availability,
                             station.time_low_unoccupied]

            # Iterate through the dictionary and the list at the same time
            index = 0
            for stat in stats_dict.values():
                # Compare the dictionary's value with the station's values
                if station_stats[index] > stat[1]:
                    stat[0] = station.name
                    stat[1] = station_stats[index]
                elif station_stats[index] == stat[1] and station.name < stat[0]:
                    stat[0] = station.name
                    stat[1] = station_stats[index]
                index += 1

        # Format dictionary
        for key in stats_dict:
            stats_dict[key] = tuple(stats_dict[key])

        return stats_dict

    def _update_active_rides_fast(self, time: datetime) -> None:
        """Update this simulation's list of active rides for the given time.

        REQUIRED IMPLEMENTATION NOTES:
        -   see Task 5 of the assignment handout
        """
        while not self.event_queue.is_empty():
            event = self.event_queue.remove()

            if event.time <= time:
                # If there are no bikes available do not start a ride
                if event.ride.start.num_bikes < 1 and \
                    isinstance(event, RideStartEvent):
                    continue

                new_events = event.process()
                for new_event in new_events:
                    self.event_queue.add(new_event)

            else:
                self.event_queue.add(event)
                return


def create_stations(stations_file: str) -> Dict[str, 'Station']:
    """Return the stations described in the given JSON data file.

    Each key in the returned dictionary is a station id,
    and each value is the corresponding Station object.
    Note that you need to call Station(...) to create these objects!

    Precondition: stations_file matches the format specified in the
                  assignment handout.

    This function should be called *before* _read_rides because the
    rides CSV file refers to station ids.
    """
    # Read in raw data using the json library.
    with open(stations_file) as file:
        raw_stations = json.load(file)

    stations = {}
    for s in raw_stations['stations']:
        # Extract the relevant fields from the raw station JSON.
        # s is a dictionary with the keys 'n', 's', 'la', 'lo', 'da', and 'ba'
        # as described in the assignment handout.
        # NOTE: all of the corresponding values are strings, and so you need
        # to convert some of them to numbers explicitly using int() or float().
        for key in s:
            if key == 's':
                name = s[key]
            elif key == 'n':
                id_num = s[key]
            elif key == 'la':
                latitude = float(s[key])
            elif key == 'lo':
                longitude = float(s[key])
            elif key == 'da':
                num_bikes = int(s[key])
            elif key == 'ba':
                empty_spots = int(s[key])
        capacity = num_bikes + empty_spots
        stations[id_num] = Station((longitude, latitude), capacity,
                                   num_bikes, name)

    return stations


def create_rides(rides_file: str,
                 stations: Dict[str, 'Station']) -> List['Ride']:
    """Return the rides described in the given CSV file.

    Lookup the station ids contained in the rides file in <stations>
    to access the corresponding Station objects.

    Ignore any ride whose start or end station is not present in <stations>.

    Precondition: rides_file matches the format specified in the
                  assignment handout.
    """
    rides = []
    with open(rides_file) as file:
        for line in csv.reader(file):
            # line is a list of strings, following the format described
            # in the assignment handout.
            #
            # Convert between a string and a datetime object
            # using the function datetime.strptime and the DATETIME_FORMAT
            # constant we defined above. Example:
            # >>> datetime.strptime('2017-06-01 8:00', DATETIME_FORMAT)
            # datetime.datetime(2017, 6, 1, 8, 0)
            start_id = line[1]
            end_id = line[3]
            if start_id in stations and end_id in stations:
                start_time = datetime.strptime(line[0], DATETIME_FORMAT)
                start_station = stations[start_id]
                end_time = datetime.strptime(line[2], DATETIME_FORMAT)
                end_station = stations[end_id]
                ride = Ride(start_station, end_station, (start_time, end_time))
                rides.append(ride)

    return rides


class Event:
    """An event in the bike share simulation.

    Events are ordered by their timestamp.

    === Attributes ===
    simulation:
        the simulation that the event is supposed to occur in
    time:
        the time at which the event should occur, which determines its priority
    """
    simulation: 'Simulation'
    time: datetime

    def __init__(self, simulation: 'Simulation', time: datetime) -> None:
        """Initialize a new event."""
        self.simulation = simulation
        self.time = time

    def __lt__(self, other: 'Event') -> bool:
        """Return whether this event is less than <other>.

        Events are ordered by their timestamp.
        """
        return self.time < other.time

    def process(self) -> List['Event']:
        """Process this event by updating the state of the simulation.

        Return a list of new events spawned by this event.
        """
        raise NotImplementedError


class RideStartEvent(Event):
    """An event corresponding to the start of a ride.

    === Attributes ===
    ride:
        the ride for which this is an event
    """
    ride: 'Ride'

    def __init__(self, simulation: 'Simulation', time: datetime,
                 ride: 'Ride') -> None:
        """Initialize a new ride start event"""
        Event.__init__(self, simulation, time)
        self.ride = ride

    def process(self) -> List['Event']:
        """Process this ride start event by adding its ride to the list
        of active rides in the simulation.

        Return a list containing one ride end event.
        """
        self.simulation.active_rides.append(self.ride)

        # The time of the ride end event is set to the time the ride ends + 1
        # If this were not the case the bikes would disappear before reaching
        # its destination.
        time = self.ride.end_time + timedelta(minutes=1)
        return [RideEndEvent(self.simulation, time, self.ride)]

class RideEndEvent(Event):
    """An event corresponding to the end of a ride.

    === Attributes ===
    ride:
        the ride for which this is an event
    """
    ride: 'Ride'

    def __init__(self, simulation: 'Simulation', time: datetime,
                 ride: 'Ride') -> None:
        """Initialize a new ride end event"""
        Event.__init__(self, simulation, time)
        self.ride = ride

    def process(self) -> List['Event']:
        """Process this ride end event by adding its ride to the list
        of active rides in the simulation.

        Return an empty list as there are no other events triggered by a 
        ride end.
        """
        # The if statement below avoids a ValueError, in case something goes
        # wrong somewhere else in the simulation.
        if self.ride in self.simulation.active_rides:
            self.simulation.active_rides.remove(self.ride)
        return []



def sample_simulation() -> Dict[str, Tuple[str, float]]:
    """Run a sample simulation. For testing purposes only."""
    sim = Simulation('test_stations.json', 'test_rides.csv')
    sim.run(datetime(2017, 6, 1, 8, 10, 0),
            datetime(2017, 6, 1, 8, 11, 0))
    return sim.calculate_statistics()


if __name__ == '__main__':
    #Uncomment these lines when you want to check your work using python_ta!
    import python_ta
    python_ta.check_all(config={
        'allowed-io': ['create_stations', 'create_rides'],
        'allowed-import-modules': [
            'doctest', 'python_ta', 'typing',
            'csv', 'datetime', 'json',
            'bikeshare', 'container', 'visualizer'
        ]
    })
    print(sample_simulation())
