FROM ubuntu:24.04

# Environment setup
ENV ROS_DISTRO=jazzy
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# Args
ARG USERNAME=devuser
ARG USER_UID=1000
ARG USER_GID=1000

# System dependencies
RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y \
    locales \
    build-essential \
    cmake \
    git \
    zsh \
    python3-pip \
    python3-venv \
    python3-dev \
    python3-setuptools \
    python3-wheel \
    python-is-python3 \
    wget \
    curl \
    vim \
    nano \
    sudo \
    software-properties-common \
    lsb-release \
    gnupg2 \
    yq \
    ca-certificates \
    && locale-gen en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

# Add ROS 2 repository and install ROS 2 Jazzy
RUN curl -sSL https://raw.githubusercontent.com/ros/rosdistro/master/ros.key \
    | gpg --dearmor -o /usr/share/keyrings/ros-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/ros-archive-keyring.gpg] http://packages.ros.org/ros2/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) main" \
    > /etc/apt/sources.list.d/ros2.list

RUN apt-get update && apt-get install -y \
    ros-jazzy-desktop-full \
    python3-colcon-common-extensions \
    python3-rosdep \
    && rm -rf /var/lib/apt/lists/*

# Initialize rosdep
RUN rosdep init && rosdep update

# Install hadolint v2.14.0 for Dockerfile linting
RUN wget -O /usr/local/bin/hadolint https://github.com/hadolint/hadolint/releases/download/v2.14.0/hadolint-Linux-x86_64 && \
    chmod +x /usr/local/bin/hadolint

# Install additional Python tools via pip
RUN pip3 install --break-system-packages \
    vcstool \
    setuptools \
    pytest \
    argcomplete \
    black \
    flake8-blind-except \
    flake8-builtins \
    flake8-class-newline \
    flake8-comprehensions \
    flake8-deprecated \
    flake8-docstrings \
    flake8-import-order \
    flake8-quotes \
    pytest-repeat \
    pytest-rerunfailures \
    sphinx \
    sphinx-rtd-theme \
    myst-parser

# Create non-root user
RUN userdel -r ubuntu || true \
    && groupdel ubuntu || true
RUN groupadd --gid ${USER_GID} ${USERNAME} && \
    useradd --uid ${USER_UID} --gid ${USER_GID} -m ${USERNAME} && \
    usermod -aG sudo ${USERNAME} && \
    echo "${USERNAME} ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/${USERNAME} && \
    chmod 0440 /etc/sudoers.d/${USERNAME}

# Switch to user context
USER ${USERNAME}
WORKDIR /home/${USERNAME}
RUN mkdir -p /home/${USERNAME}/workspace
