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
    my $self = $class->SUPER::new(@items);

    if ($config && (!ref($config) || ref($config) ne "HASH")) {
        require Carp;
        Carp::croak("invalid first argument to constructor -- must be a hashref with any configuration options");
    }

    # make sure that maxsize is actually a number
    my $maxsize = ($config) ? $config->{'maxsize'} : undef;
    $self->{'MAXSIZE'} = $self->_validate_maxsize($maxsize);

    # determine what type of action we'll take on exceeding our max size
    # 1. raise an exception (die)
    # 2. warn and reject entire addition/insertion
    # 3. silently reject entire addition/insertion
    # 4. warn, process addition/insertion and then truncate to max size
    # 5. silently process addition/insertion and then truncate to max size
    my $on_maxsize = ($config) ? $config->{'on_maxsize'} : undef;
    $self->{'ON_MAXSIZE'} = $self->_validate_on_maxsize($on_maxsize || 'silent_truncate');

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
    if (defined($self->{'MAXSIZE'}) && $self->{'MAXSIZE'} > 0) {
        if ((scalar(@{$queue}) + scalar(@_)) > $self->{'MAXSIZE'} &&
            $self->{'ON_MAXSIZE'} =~ /^(die|warn_and_reject|silent_reject|warn_and_truncate)$/ix) {
            if ($1 =~ /^warn_and_truncate$/ix) {
                warn "queue exceeding its maximum size: truncating\n";
            } elsif ($1 =~ /^silent_reject$/ix) {
                return;
            } elsif ($1 =~ /^warn_and_reject$/ix) {
                warn "not enqueuing new items: queue would exceed its maximum size\n";
                return;
            } elsif ($1 =~ /^die$/ix) {
                die "not enqueuing new items: queue would exceed its maximum size\n";
            }
        }

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
    if (defined($self->{'MAXSIZE'}) && $self->{'MAXSIZE'} > 0) {
        if ((scalar(@{$queue}) + scalar(@_) + scalar(@tmp)) > $self->{'MAXSIZE'} &&
            $self->{'ON_MAXSIZE'} =~ /^(die|warn_and_reject|silent_reject|warn_and_truncate)$/ix) {
            if ($1 =~ /^warn_and_truncate$/ix) {
                warn "queue exceeding its maximum size: truncating\n";
            } elsif ($1 =~ /^silent_reject$/ix) {
                # reset queue before dying
                push(@{$queue}, @tmp);
                return;
            } elsif ($1 =~ /^warn_and_reject$/ix) {
                # reset queue before dying
                push(@{$queue}, @tmp);
                warn "not inserting new items: queue would exceed its maximum size\n";
                return;
            } elsif ($1 =~ /^die$/ix) {
                # reset queue before dying
                push(@{$queue}, @tmp);
                die "not inserting new items: queue would exceed its maximum size\n";
            }
        }

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

sub _validate_maxsize {
    my ($self, $maxsize) = @_;

    if (defined($maxsize) && (!looks_like_number($maxsize) || (int($maxsize) != $maxsize) || ($maxsize < 1))) {
        require Carp;
        my ($method) = (caller(1))[3];
        my $class_name = ref($self);
        $method =~ s/${class_name}:://;
        Carp::croak("Invalid 'maxsize' argument ($maxsize) to '$method' method");
    }

    return $maxsize;
}

sub _validate_on_maxsize {
    my ($self, $on_maxsize) = @_;

    if (defined($on_maxsize) && ($on_maxsize !~ /^(?:die|warn_and_reject|silent_reject|warn_and_truncate|silent_truncate)$/ix)) {
        require Carp;
        my ($method) = (caller(1))[3];
        my $class_name = ref($self);
        $method =~ s/${class_name}:://;
        Carp::croak("Invalid 'on_maxsize' argument ($on_maxsize) to '$method' method");
    }

    return $on_maxsize;
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
