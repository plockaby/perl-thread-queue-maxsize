package Thread::Queue::MaxSize;

use strict;
use warnings;

our $VERSION = '1.0.0';
$VERSION = eval $VERSION;

use threads::shared 1.21;
use Scalar::Util 1.10 qw(looks_like_number);

# carp errors from threads::shared calls should complain about caller
our @CARP_NOT = ("threads::shared");

sub new {
    my $class = shift;

    my $config = shift;
    if ($config && (!ref($config) || ref($config) ne "HASH")) {
        croak("invalid first argument (${config})");
    }

    # make sure that maxsize is actually a number
    my $maxsize = ($config) ? $config->{'maxsize'} : undef;
    if (defined($maxsize) && (!looks_like_number($maxsize) || (int($maxsize) != $maxsize) || ($maxsize < 1))) {
        croak("invalid 'maxsize' argument (${maxsize})");
    }

    my @queue :shared = map { shared_clone($_) } @_;
    my %self :shared = (
        '_queue'   => \@queue,
        '_maxsize' => $maxsize,
        '_ended'   => 0,
    );
    return bless(\%self, $class);
}

# add items to the tail of a queue
sub enqueue {
    my $self = shift;
    lock(%$self);

    if ($self->{'_ended'}) {
        require Carp;
        Carp::croak("'enqueue' method called on queue that has been 'end'ed");
    }

    my $queue = $self->{'_queue'};

    # queue can't be too big so shift the oldest things off if necessary
    if (defined($self->{'_maxsize'})) {
        # remove things already on the queue
        while (scalar(@{$queue}) && (scalar(@{$queue}) + scalar(@_)) > $self->{'_maxsize'}) {
            shift(@{$queue});
        }

        # if we've already removed everything off of the queue and we're still
        # over maxsize then take things off of the list of new items
        while (scalar(@_) && (scalar(@_)) > $self->{'_maxsize'}) {
            shift(@_);
        }
    }

    push(@{$queue}, map { shared_clone($_) } @_) and cond_signal(%$self);
}

# return a count of the number of items on a queue
sub pending {
    my $self = shift;
    lock(%$self);
    return if ($self->{'_ended'} && ! @{$self->{'_queue'}});
    return scalar(@{$self->{'_queue'}});
}

# indicate that no more data will enter the queue
sub end {
    my $self = shift;
    lock(%$self);

    # no more data is coming
    $self->{'_ended'} = 1;

    # try to release at least one blocked thread
    cond_signal(%$self);
}

# return 1 or more items from the head of a queue, blocking if needed
sub dequeue {
    my $self = shift;
    lock(%$self);
    my $queue = $self->{'_queue'};

    my $count = @_ ? _validate_count(shift) : 1;

    # wait for requisite number of items
    cond_wait(%$self) while ((@{$queue} < $count) && ! $self->{'_ended'});
    cond_signal(%$self) if ((@{$queue} > $count) || $self->{'_ended'});

    # if no longer blocking, try getting whatever is left on the queue
    return $self->dequeue_nb($count) if ($self->{'_ended'});

    # return single item
    return shift(@{$queue}) if ($count == 1);

    # return multiple items
    my @items;
    push(@items, shift(@{$queue})) for (1..$count);
    return @items;
}

# return items from the head of a queue with no blocking
sub dequeue_nb {
    my $self = shift;
    lock(%$self);
    my $queue = $self->{'_queue'};

    my $count = @_ ? _validate_count(shift) : 1;

    # return single item
    return shift(@{$queue}) if ($count == 1);

    # return multiple items
    my @items;
    for (1..$count) {
        last if (! @{$queue});
        push(@items, shift(@{$queue}));
    }
    return @items;
}

# return items from the head of a queue, blocking if needed up to a timeout
sub dequeue_timed {
    my $self = shift;
    lock(%$self);
    my $queue = $self->{'_queue'};

    # timeout may be relative or absolute
    my $timeout = @_ ? _validate_timeout(shift) : -1;

    # convert to an absolute time for use with cond_timedwait()
    $timeout += time() if ($timeout < 32000000); # more than one year

    my $count = @_ ? _validate_count(shift) : 1;

    # wait for requisite number of items, or until timeout
    while ((@{$queue} < $count) && ! $self->{'_ended'}) {
        last if (!cond_timedwait(%$self, $timeout));
    }
    cond_signal(%$self) if ((@{$queue} > $count) || $self->{'_ended'});

    # get whatever we need off the queue if available
    return $self->dequeue_nb($count);
}

# return an item without removing it from a queue
sub peek {
    my $self = shift;
    lock(%$self);
    my $index = @_ ? _validate_index(shift) : 0;
    return $self->{'_queue'}[$index];
}

# insert items anywhere into a queue
sub insert {
    my $self = shift;
    lock(%$self);

    if ($self->{'_ended'}) {
        require Carp;
        Carp::croak("'insert' method called on queue that has been 'end'ed");
    }

    my $queue = $self->{'_queue'};

    my $index = _validate_index(shift);

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
    if (defined($self->{'_maxsize'})) {
        # remove things already on the queue
        while (scalar(@{$queue}) && (scalar(@{$queue}) + scalar(@_) + scalar(@tmp)) > $self->{'_maxsize'}) {
            shift(@{$queue});
        }

        # if we've already removed everything off of the queue and we're still
        # over maxsize then take things off of the list of new items
        while (scalar(@_) && (scalar(@_) + scalar(@tmp)) > $self->{'_maxsize'}) {
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

# remove items from anywhere in a queue
sub extract {
    my $self = shift;
    lock(%$self);
    my $queue = $self->{'_queue'};

    my $index = @_ ? _validate_index(shift) : 0;
    my $count = @_ ? _validate_count(shift) : 1;

    # support negative indices
    if ($index < 0) {
        $index += @{$queue};
        if ($index < 0) {
            $count += $index;
            return if ($count <= 0);           # beyond the head of the queue
            return $self->dequeue_nb($count);  # extract from the head
        }
    }

    # dequeue items from $index+$count onward
    my @tmp = ();
    while (@{$queue} > ($index+$count)) {
        unshift(@tmp, pop(@{$queue}))
    }

    # extract desired items
    my @items = ();
    unshift(@items, pop(@{$queue})) while (@{$queue} > $index);

    # add back any removed items
    push(@{$queue}, @tmp);

    # return single item
    return $items[0] if ($count == 1);

    # return multiple items
    return @items;
}

# check value of the requested index
sub _validate_index {
    my $index = shift;

    if (!defined($index) || !looks_like_number($index) || (int($index) != $index)) {
        require Carp;
        my ($method) = (caller(1))[3];
        $method =~ s/^Thread::Queue::MaxSize//;
        $index = 'undef' if (! defined($index));
        Carp::croak("Invalid 'index' argument (${index}) to '${method}' method");
    }

    return $index;
}

# check value of the requested count
sub _validate_count {
    my $count = shift;

    if (!defined($count) || !looks_like_number($count) || (int($count) != $count) || ($count < 1)) {
        require Carp;
        my ($method) = (caller(1))[3];
        $method =~ s/^Thread::Queue::MaxSize//;
        $count = 'undef' if (! defined($count));
        Carp::croak("Invalid 'count' argument (${count}) to '${method}' method");
    }

    return $count;
}

# check value of the requested timeout
sub _validate_timeout {
    my $timeout = shift;

    if (!defined($timeout) || !looks_like_number($timeout)) {
        require Carp;
        my ($method) = (caller(1))[3];
        $method =~ s/^Thread::Queue::MaxSize//;
        $timeout = 'undef' if (! defined($timeout));
        Carp::croak("Invalid 'timeout' argument (${timeout}) to '${method}' method");
    }

    return $timeout;
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
