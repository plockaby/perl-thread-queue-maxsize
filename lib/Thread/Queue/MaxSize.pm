package Thread::Queue::MaxSize;

use strict;
use warnings;

our $VERSION = '1.0.0';
$VERSION = eval $VERSION;

use parent qw(Thread::Queue);

use threads::shared 1.21;
use Scalar::Util 1.10 qw(looks_like_number);

sub new {
    my ($class, $config, @items) = @_;

    if ($config && (!ref($config) || ref($config) ne "HASH")) {
        require Carp;
        Carp::croak("invalid first argument to constructor -- must be a hashref with any configuration options");
    }

    # make sure that maxsize is actually a number
    my $maxsize = ($config) ? $config->{'maxsize'} : undef;
    if (defined($maxsize) && (!looks_like_number($maxsize) || (int($maxsize) != $maxsize) || ($maxsize < 1))) {
        require Carp;
        croak("invalid 'maxsize' argument (${maxsize})");
    }

    # determine what type of action we'll take on exceeding our max size
    # TODO

    my $self = $class->SUPER::new(@items);
    $self->{'MAXSIZE'} = $maxsize;
    return $self;
}

# add items to the tail of a queue
sub enqueue {
    my $self = shift;
    lock(%$self);

    if ($self->{'ENDED'}) {
        require Carp;
        Carp::croak("'enqueue' method called on queue that has been 'end'ed");
    }

    my $queue = $self->{'queue'};

    # queue can't be too big so shift the oldest things off if necessary
    if (defined($self->{'MAXSIZE'})) {
        # remove things already on the queue
        while (scalar(@{$queue}) && (scalar(@{$queue}) + scalar(@_)) > $self->{'MAXSIZE'}) {
            shift(@{$queue});
        }

        # if we've already removed everything off of the queue and we're still
        # over maxsize then take things off of the list of new items
        while (scalar(@_) && (scalar(@_)) > $self->{'MAXSIZE'}) {
            shift(@_);
        }
    }

    push(@{$queue}, map { shared_clone($_) } @_) and cond_signal(%$self);
}

# insert items anywhere into a queue
sub insert {
    my $self = shift;
    lock(%$self);

    if ($self->{'ENDED'}) {
        require Carp;
        Carp::croak("'insert' method called on queue that has been 'end'ed");
    }

    my $queue = $self->{'queue'};

    my $index = $self->_validate_index(shift);

    # make sure we have something to insert
    return unless @_;

    # support negative indices
    if ($index < 0) {
        $index += @{$queue};
        $index = 0 if ($index < 0);
    }

    # dequeue items from $index onward
    my @tmp = ();
    while (@{$queue} > $index) {
        unshift(@tmp, pop(@{$queue}))
    }

    # queue can't be too big so shift the oldest things off if necessary
    if (defined($self->{'MAXSIZE'})) {
        # remove things already on the queue
        while (scalar(@{$queue}) && (scalar(@{$queue}) + scalar(@_) + scalar(@tmp)) > $self->{'MAXSIZE'}) {
            shift(@{$queue});
        }

        # if we've already removed everything off of the queue and we're still
        # over maxsize then take things off of the list of new items
        while (scalar(@_) && (scalar(@_) + scalar(@tmp)) > $self->{'MAXSIZE'}) {
            shift(@_);
        }
    }

    # add new items to the queue
    push(@{$queue}, map { shared_clone($_) } @_);

    # add previous items back onto the queue
    push(@{$queue}, @tmp);

    # soup's up
    cond_signal(%$self);
}

1;

=head1 NAME

Thread::Queue::MaxSize - Thread-safe queues with an upper bound

=head1 VERSION

This document describes Thread::Queue::MaxSize version 1.0.0

=head1 SYNOPSIS

    use strict;
    use warnings;

    use threads;
    use Thread::Queue::MaxSize;

    # create a new empty queue with no max limit
    my $q = Thread::Queue::MaxSize->new();

    # create a new empty queue that will only ever store 1000 entries
    my $q = Thread::Queue::MaxSize->new({ maxsize => 1000 });

    # create a new empty queue with no max limit and some default values
    my $q = Thread::Queue::MaxSize->new({}, $foo, $bar, @qwerty);

    # create a new empty queue with some default values that will only ever
    # store 1000 entries
    my $q = Thread::Queue::MaxSize->new({ maxsize => 1000 }, $foo, $bar, @qwerty);

=head1 DESCRIPTION

This is a variation on L<Thread::Queue> that will enforce an upper bound on the
number of entries that can be enqueued. This can be used to prevent memory use
from exploding on a queue that might never empty.

If new entries are added to the queue that cause the queue to exceed the
configured size, the oldest entries will be dropped off the end. This may cause
your program to miss some entries on the queue but that's how it works.

=head1 SEE ALSO

L<Thread::Queue>, L<threads>, L<threads::shared>

=head1 MAINTAINER

Paul Lockaby S<E<lt>plockaby AT cpan DOT orgE<gt>>

=head1 CREDIT

Large huge portions of this module are directly from L<Thread::Queue> which is
maintained by Jerry D. Hedden.

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut
