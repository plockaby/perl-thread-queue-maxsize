#!/usr/bin/env perl

use strict;
use warnings FATAL => 'all';

use Test::More 'tests' => 24;

use Thread::Queue::MaxSize;

# Queue up items
my $max = 12;
my $q = Thread::Queue::MaxSize->new({ maxsize => $max }, 1 .. 10);
ok($q, 'New queue');

is($q->pending(), 10, 'Initial queue size');
is($q->peek(), 1, 'Initial first value');
$q->enqueue('asdf');
is($q->pending(), 11, 'Queue size after one');
is($q->peek(), 1, 'First value after one');
$q->enqueue('fdsa');
is($q->pending(), 12, 'Queue size after two');
is($q->peek(), 1, 'First value after two');
$q->enqueue('qwerty');
is($q->pending(), 12, 'Queue size after three');
is($q->peek(), 2, 'First value after three');

# create a queue that can only have ten items in it
# then put 8 items into it
$max = 10;
$q = Thread::Queue::MaxSize->new({ maxsize => $max }, 1 .. 8);
is($q->pending(), 8, 'Initial queue size');

# add three 12 things to it
$q->enqueue($_) for (1 .. 12);
is($q->pending(), 10, 'Queue size after adding twelve things to a size ten queue');
is($q->peek(), 3, 'Verify two oldest thinges were dequeued from the new list');

# create a queue that can only have ten items in it
# then put 8 items into it
$max = 10;
$q = Thread::Queue::MaxSize->new({ maxsize => $max}, 1 .. 8);
is($q->pending(), 8, 'Initial queue size');

# insert four things into the middle of it
# this should remove all three at the beginning
# plus one on the new list
$q->insert(3, (1 .. 6));
is($q->pending(), 10, 'Queue size after inserting four things into the middle');
is($q->dequeue(), 2, 'Queue value 1');
is($q->dequeue(), 3, 'Queue value 2');
is($q->dequeue(), 4, 'Queue value 3');
is($q->dequeue(), 5, 'Queue value 4');
is($q->dequeue(), 6, 'Queue value 5');
is($q->dequeue(), 4, 'Queue value 6');
is($q->dequeue(), 5, 'Queue value 7');
is($q->dequeue(), 6, 'Queue value 8');
is($q->dequeue(), 7, 'Queue value 9');
is($q->dequeue(), 8, 'Queue value 10');

