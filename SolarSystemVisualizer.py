import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import matplotlib.patches as patches


class CelestialBody:
    def __init__(self, name, mass, position, velocity, color, size):
        self.name = name
        self.mass = mass  # kg
        self.position = np.array(position, dtype=float)  # m
        self.velocity = np.array(velocity, dtype=float)  # m/s
        self.color = color
        self.size = size  # 显示大小
        self.trajectory = [self.position.copy()]

    def update_position(self, dt):
        self.position += self.velocity * dt
        self.trajectory.append(self.position.copy())


class SolarSystem:
    def __init__(self):
        self.bodies = []
        self.G = 6.67430e-11  # 引力常数 (m³/kg/s²)

    def add_body(self, body):
        self.bodies.append(body)

    def calculate_force(self, body1, body2):
        # 计算两个天体之间的引力
        r_vec = body2.position - body1.position
        r = np.linalg.norm(r_vec)
        if r == 0:
            return np.array([0.0, 0.0])
        force_magnitude = self.G * body1.mass * body2.mass / (r ** 2)
        force_vector = force_magnitude * r_vec / r
        return force_vector

    def update(self, dt):
        # 计算所有天体受到的合力
        forces = [np.array([0.0, 0.0]) for _ in self.bodies]

        for i in range(len(self.bodies)):
            for j in range(i + 1, len(self.bodies)):
                force = self.calculate_force(self.bodies[i], self.bodies[j])
                forces[i] += force
                forces[j] -= force  # 牛顿第三定律

        # 更新速度和位置
        for i, body in enumerate(self.bodies):
            acceleration = forces[i] / body.mass
            body.velocity += acceleration * dt
            body.update_position(dt)


def create_solar_system():
    # 创建太阳系模型（简化版，比例已调整以便观察）
    solar_system = SolarSystem()

    # 太阳
    sun = CelestialBody(
        name="Sun",
        mass=1.989e30,
        position=[0, 0],
        velocity=[0, 0],
        color='yellow',
        size=100
    )

    # 水星
    mercury = CelestialBody(
        name="Mercury",
        mass=3.3011e23,
        position=[5.79e10, 0],
        velocity=[0, 4.74e4],
        color='gray',
        size=10
    )

    # 金星
    venus = CelestialBody(
        name="Venus",
        mass=4.8675e24,
        position=[1.08e11, 0],
        velocity=[0, 3.50e4],
        color='orange',
        size=15
    )

    # 地球
    earth = CelestialBody(
        name="Earth",
        mass=5.972e24,
        position=[1.50e11, 0],
        velocity=[0, 2.98e4],
        color='blue',
        size=15
    )

    # 火星
    mars = CelestialBody(
        name="Mars",
        mass=6.4171e23,
        position=[2.28e11, 0],
        velocity=[0, 2.41e4],
        color='red',
        size=12
    )

    # 木星
    jupiter = CelestialBody(
        name="Jupiter",
        mass=1.898e27,
        position=[7.78e11, 0],
        velocity=[0, 1.31e4],
        color='brown',
        size=50
    )

    solar_system.add_body(sun)
    solar_system.add_body(mercury)
    solar_system.add_body(venus)
    solar_system.add_body(earth)
    solar_system.add_body(mars)
    solar_system.add_body(jupiter)

    return solar_system


class SolarSystemVisualizer:
    def __init__(self, solar_system):
        self.solar_system = solar_system
        self.fig, self.ax = plt.subplots(figsize=(12, 10))
        self.ax.set_facecolor('black')
        self.ax.set_aspect('equal')
        self.ax.set_title('Solar System Simulation', color='white')

        # 调整坐标轴范围以适应所有行星轨道
        self.ax.set_xlim(-8e11, 8e11)
        self.ax.set_ylim(-8e11, 8e11)

        # 隐藏坐标轴刻度
        self.ax.set_xticks([])
        self.ax.set_yticks([])

        # 创建绘图对象
        self.body_plots = []
        self.trajectory_plots = []

        # 初始化天体和轨迹绘制对象
        for body in self.solar_system.bodies:
            # 绘制天体
            body_plot = self.ax.plot([], [], 'o', markersize=body.size / 10,
                                     color=body.color, label=body.name)[0]
            self.body_plots.append(body_plot)

            # 绘制轨迹
            trajectory_plot = self.ax.plot([], [], '-', linewidth=0.5,
                                           color=body.color, alpha=0.7)[0]
            self.trajectory_plots.append(trajectory_plot)

        # 添加图例
        legend = self.ax.legend(loc='upper right', facecolor='black', edgecolor='white')
        for text in legend.get_texts():
            text.set_color('white')

    def update_frame(self, frame):
        # 每帧更新太阳系状态
        # 使用较小的时间步长进行多次计算以提高精度
        dt = 3600 * 24  # 1天时间步长
        for _ in range(10):  # 每帧计算10次
            self.solar_system.update(dt / 10)

        # 更新天体位置
        for i, body in enumerate(self.solar_system.bodies):
            self.body_plots[i].set_data([body.position[0]], [body.position[1]])

            # 更新轨迹（只保留最近的轨道部分）
            if len(body.trajectory) > 500:  # 限制轨迹长度
                body.trajectory.pop(0)

            traj_x = [pos[0] for pos in body.trajectory]
            traj_y = [pos[1] for pos in body.trajectory]
            self.trajectory_plots[i].set_data(traj_x, traj_y)

        return self.body_plots + self.trajectory_plots

    def animate(self):
        self.animation = FuncAnimation(
            self.fig,
            self.update_frame,
            frames=1000,
            interval=50,  # 每50毫秒更新一帧
            blit=True
        )
        plt.show()


# 运行模拟
if __name__ == "__main__":
    # 创建太阳系
    solar_system = create_solar_system()

    # 创建可视化界面
    visualizer = SolarSystemVisualizer(solar_system)

    # 开始动画
    visualizer.animate()
