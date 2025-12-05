import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation


class HeartParticleAnimation:
    def __init__(self, num_particles=2000):
        """
        初始化心脏粒子动画

        Args:
            num_particles: 粒子数量
        """
        self.num_particles = num_particles
        self.time = 0
        self.num_layers = 5

        # 设置图形
        self.fig, self.ax = plt.subplots(figsize=(10, 8))
        self.ax.set_facecolor('#0c0c1f')
        self.ax.set_aspect('equal')
        self.ax.set_title('3D Pulsating Heart with Particles', color='white', fontsize=16)

        # 隐藏坐标轴
        self.ax.set_xticks([])
        self.ax.set_yticks([])

        # 设置坐标轴范围确保爱心可见
        self.ax.set_xlim(-20, 20)
        self.ax.set_ylim(-15, 15)

        # 创建心形轮廓上的粒子
        self.create_heart_particles()

        # 初始化多层粒子绘制对象（创建3D效果）
        self.particle_layers = []

        # 创建多个层次的粒子
        for i in range(self.num_layers):
            layer = self.ax.scatter(
                [], [],
                c=[],
                s=[],
                alpha=0.8 - i * 0.15,
                edgecolors='none'
            )
            self.particle_layers.append(layer)

        # 初始化心电图线条
        self.ecg_line, = self.ax.plot([], [], 'lime', linewidth=2)
        self.ecg_time_points = np.linspace(0, 4 * np.pi, 100)  # 心电图时间点
        self.ecg_history = []  # 存储心电图历史值

    def heart_function(self, t):
        """
        心形函数参数方程
        """
        x = 16 * np.sin(t) ** 3
        y = 13 * np.cos(t) - 5 * np.cos(2 * t) - 2 * np.cos(3 * t) - np.cos(4 * t)
        return x, y

    def generate_ecg_wave(self, t):
        """
        生成心电图波形
        """
        # 基础心电图波形，模仿QRS复合波
        p_wave = 0.3 * np.sin(t * 0.5)  # P波
        qrs_complex = 1.0 * np.sin(t * 3) * np.exp(-((t % (2 * np.pi) - np.pi) ** 2) / 0.5)  # QRS复合波
        t_wave = 0.25 * np.sin(t * 0.6 + np.pi / 4)  # T波

        # 组合波形
        ecg = p_wave + qrs_complex + t_wave

        # 根据心跳节奏调整幅度
        heartbeat_intensity = np.sin(self.time * 2) * 0.3 + 0.7
        return ecg * heartbeat_intensity

    def create_heart_particles(self):
        """
        在心形轮廓上创建粒子
        """
        # 生成心形轮廓上的点
        t_values = np.linspace(0, 2 * np.pi, self.num_particles)
        x_vals, y_vals = self.heart_function(t_values)

        # 添加适度随机偏移使效果更自然（减少偏移量以保持心形）
        noise_x = np.random.normal(0, 0.8, self.num_particles)
        noise_y = np.random.normal(0, 0.8, self.num_particles)

        self.particles_x = x_vals + noise_x
        self.particles_y = y_vals + noise_y

        # 为每个粒子分配层次（实现3D深度感）
        self.particle_layers_idx = np.random.randint(0, self.num_layers, self.num_particles)

        # 设置粒子颜色（使用多种红色调，根据层次调整亮度）
        base_colors = np.linspace(0.3, 1, self.num_particles)  # 调整颜色范围
        color_variation = np.random.uniform(0.8, 1.2, self.num_particles)
        self.particles_colors_base = np.clip(base_colors * color_variation, 0.1, 1)

        # 根据层次调整颜色亮度（近亮远暗）
        self.particles_colors = []
        for i, base_color in enumerate(self.particles_colors_base):
            layer_factor = (self.particle_layers_idx[i] + 1) / self.num_layers
            adjusted_color = base_color * (1.2 - 0.5 * layer_factor)
            self.particles_colors.append(plt.cm.Reds(np.clip(adjusted_color, 0.1, 1)))

        # 设置粒子大小（根据层次调整大小，近大远小）
        base_sizes = np.random.uniform(5, 25, self.num_particles)  # 调整大小范围
        self.particles_size = []
        for i, base_size in enumerate(base_sizes):
            layer_factor = (self.particle_layers_idx[i] + 1) / self.num_layers
            adjusted_size = base_size * (1.3 - 0.5 * layer_factor)
            self.particles_size.append(adjusted_size)

        # 存储原始位置用于动画
        self.original_x = self.particles_x.copy()
        self.original_y = self.particles_y.copy()

    def update_frame(self, frame):
        """
        更新每一帧的动画
        """
        self.time += 0.1

        # 计算心跳节奏
        pulse = np.sin(self.time * 2) * 0.15 + 1.0  # 调整脉动幅度
        beat = np.sin(self.time * 8) * 0.08
        scale = pulse + beat

        # 应用心跳缩放效果
        self.particles_x = self.original_x * scale
        self.particles_y = self.original_y * scale

        # 添加粒子轻微震动效果
        vibration_x = np.random.normal(0, 0.2, self.num_particles)
        vibration_y = np.random.normal(0, 0.2, self.num_particles)

        # 更新粒子位置
        updated_x = self.particles_x + vibration_x
        updated_y = self.particles_y + vibration_y

        # 分层更新粒子（实现3D效果）
        artists = []
        for layer_idx in range(self.num_layers):
            # 获取当前层的所有粒子索引
            layer_particle_indices = np.where(self.particle_layers_idx == layer_idx)[0]

            if len(layer_particle_indices) > 0:
                # 获取当前层粒子的数据
                layer_x = updated_x[layer_particle_indices]
                layer_y = updated_y[layer_particle_indices]
                layer_sizes = [self.particles_size[i] for i in layer_particle_indices]

                # 根据心跳调整当前层粒子的大小
                layer_scale_factor = 1.0 + 0.2 * np.sin(self.time * 3) * (1.0 - layer_idx / self.num_layers * 0.5)
                dynamic_sizes = np.array(layer_sizes) * layer_scale_factor

                # 更新当前层
                self.particle_layers[layer_idx].set_offsets(np.column_stack((layer_x, layer_y)))
                self.particle_layers[layer_idx].set_sizes(dynamic_sizes)
                # 更新颜色
                layer_colors = [self.particles_colors[i] for i in layer_particle_indices]
                self.particle_layers[layer_idx].set_color(layer_colors)

                artists.append(self.particle_layers[layer_idx])

        # 根据心跳改变整体透明度
        overall_alpha = 0.6 + 0.4 * np.sin(self.time * 4)
        for i, layer in enumerate(self.particle_layers):
            layer_alpha = (0.8 - i * 0.15) * overall_alpha
            layer.set_alpha(max(layer_alpha, 0.2))

        # 更新心电图
        current_ecg_value = self.generate_ecg_wave(self.time)
        self.ecg_history.append(current_ecg_value)

        # 保持心电图历史长度固定
        if len(self.ecg_history) > len(self.ecg_time_points):
            self.ecg_history.pop(0)

        # 更新心电图线条数据
        if len(self.ecg_history) > 1:
            x_ecg = np.linspace(-18, 18, len(self.ecg_history))
            y_ecg = np.array(self.ecg_history) * 2  # 放大ECG信号
            self.ecg_line.set_data(x_ecg, y_ecg)
            artists.append(self.ecg_line)

        return artists

    def animate(self):
        """
        开始动画
        """
        self.animation = FuncAnimation(
            self.fig,
            self.update_frame,
            frames=1000,
            interval=50,
            blit=True
        )
        plt.tight_layout()
        plt.show()


# 运行粒子化心脏动画
if __name__ == "__main__":
    heart_animation = HeartParticleAnimation(num_particles=2000)
    heart_animation.animate()
